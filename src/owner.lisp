;;;; src/owner.lisp -- the OWNER LOOP: one thread owns some state; everything else reaches it by
;;;; message, and the owner does the work when it PUMPS.
;;;;
;;;; THE MODEL.  modus threads share no state: a thread may not store an object it made into
;;;; anything another thread can reach.  A program whose state is hash tables and records that many
;;;; threads used to change (a node's peers, a ledger, a chain) cannot keep that shape there.  So,
;;;; on every Lisp:
;;;;
;;;;   - ONE OWNER THREAD (START-OWNER makes the calling thread the owner; normally the main
;;;;     thread) holds the state and runs everything that reads or changes it.
;;;;   - What used to be a thread is a LANE (a queue of work run one item at a time) or a TIMER
;;;;     (EVERY-SECONDS, AFTER-SECONDS).  Both run on the owner when it PUMPS.
;;;;   - The owner pumps whenever it waits: PAUSE-FOR, WAIT-UNTIL and PUMP-UNTIL replace SLEEP and
;;;;     CONDITION-WAIT.  So a lane waiting for an answer lets the other lanes and the timers run
;;;;     meanwhile: waits overlap as they did with threads.  Computation does not: it is one thread.
;;;;   - OTHER THREADS never touch the owner's state: OWNER-CALL sends a named function and plain
;;;;     arguments to the owner, which runs it while pumping and sends the value back; OWNER-POST
;;;;     does the same without waiting for an answer.  Blocking work the owner should not wait on
;;;;     (an RPC to another process) goes to a WORK THREAD the same way in reverse (RUN-WORK).
;;;;     Everything travels on bobbin channels, which copy it.
;;;;   - OTHER EVENT SOURCES (a net thread's event channel, say) are drained by the same pump
;;;;     (ADD-OWNER-SOURCE), so one wait serves every source: a lane waiting in PAUSE-FOR delivers
;;;;     network input, and a network handshake waiting in a source's own read runs lanes and timers.
;;;;     A source's producer calls OWNER-WAKE after queuing input, so an idle owner wakes at once.
;;;;
;;;; THERE IS ONE OWNER LOOP PER PROCESS.  Libraries that each need an owner share it: that is the
;;;; point (cl-consensus's net layer and cl-deposits' lanes, loaded together, are served by one
;;;; pump).  Its channels are made by the first START-OWNER, on that thread -- on modus that must
;;;; be a thread every other thread may name (the main thread): call START-OWNER there first.
;;;;
;;;; WAITS NEST.  A wait pumps, the pump runs another lane's item, and that item may wait in its
;;;; turn: the first wait cannot return before the second does.  So a long wait inside a short one
;;;; holds the short one back, and work that would have WAITED for a lock another task holds cannot
;;;; wait here (the holder is suspended below it on the same stack).  OWNER-LOCK is that lock:
;;;; re-entrant for the task holding it; for any other task WITH-OWNER-LOCK signals OWNER-BUSY at
;;;; once, and a lane item, timer run or OWNER-CALL refused that way is run again a little later.
;;;; Nesting is bounded (+MAX-DEPTH+): past it a pump only delivers messages and drains sources.
;;;;
;;;; OFF THE OWNER, PUMP only waits -- except, on a Lisp whose threads share memory (SBCL, not
;;;; modus), it drains the sources that said :ANY-THREAD T, so threads waiting on a source share
;;;; its work (cl-consensus's IBD fetchers each run their peers' handlers).
;;;;
;;;; NOT CALLED "PAUSE": modus compiles (pause ...) as its spin-wait intrinsic whatever package the
;;;; symbol is in, so a function named PAUSE is never called there.

(in-package #:bobbin.owner)

(defvar *owner-thread* nil "The owner thread (START-OWNER), or NIL before there is one.")

(defun owner-thread () *owner-thread*)
(defun owner-p ()
  "True on the owner thread."
  (and *owner-thread* (eq sb-thread:*current-thread* *owner-thread*)))

(defconstant +max-depth+ 48 "Nested pumps past this deliver messages only.")
(defvar *depth* 0 "How many pumps are running on the owner's stack.")
(defun pump-depth () *depth*)

(defvar *task* :top
  "The task running now: :TOP for the owner's own code, else the lane, timer, or (:CALL fname) /
   (:POST fname) a pump started.  OWNER-LOCK is re-entrant per task.")

(defun %now () (/ (get-internal-real-time) internal-time-units-per-second))
(defun %deadline (seconds) (+ (get-internal-real-time) (round (* seconds internal-time-units-per-second))))
(defun %left (deadline) (/ (- deadline (get-internal-real-time)) internal-time-units-per-second))

(defun %warn (fmt &rest args)
  (ignore-errors (format *error-output* "~&[owner] ~?~%" fmt args) (force-output *error-output*)))

;;; ---------------------------------------------------------------------------
;;; The owner's channels: made once, by the first START-OWNER (on modus a channel is never
;;; reclaimed, and one made by the main thread may be named by every thread).

(defparameter *owner-call-slots* 16
  "How many OWNER-CALLs may be in flight at once.  Read by the first START-OWNER.")
(defparameter *owner-message-bytes* (* 8 1024 1024)
  "Capacity of the owner's inbox and of each OWNER-CALL reply channel: it bounds one message (an
   OWNER-CALL's arguments or value).  Read by the first START-OWNER.")

(defvar *inbox* nil "CHANNEL: (:call ...) (:post ...) (:work ...) (:wake) to the owner.")
(defvar *call-free* nil "CHANNEL holding the numbers of the OWNER-CALL slots not in use.")
(defvar *call-replies* nil "Vector: OWNER-CALL slot -> its reply CHANNEL.")
(defvar *start-lock* (sb-thread:make-mutex :name "bobbin owner start"))

(defvar *wake* (make-array 1 :initial-element 0)
  "Element 0 says what the owner is doing: 0 working, 1 idle in PUMP waiting on its inbox, 2 idle
   waiting in a source's DRAIN (*WAITED*).  OWNER-WAKE reads it to know whom to ring.  A fixnum in
   a vector made at load -- on modus any thread may store a fixnum into it.")

(defun start-owner ()
  "Make the calling thread the owner, making the owner's channels if this is the first time.  On
   modus call it first on the main thread.  T."
  (sb-thread:with-mutex (*start-lock*)
    (unless *inbox*
      (let ((free (bobbin:make-channel :capacity 4096 :name "bobbin owner-call slots"))
            (replies (make-array *owner-call-slots*)))
        (dotimes (i *owner-call-slots*)
          (setf (svref replies i)
                (bobbin:make-channel :capacity *owner-message-bytes* :name "bobbin owner-call reply"))
          (bobbin:channel-send free i))
        (setf *call-free* free
              *call-replies* replies
              *inbox* (bobbin:make-channel :capacity *owner-message-bytes* :name "bobbin owner inbox"))))
    (setf *owner-thread* sb-thread:*current-thread*))
  t)

;;; ---------------------------------------------------------------------------
;;; Sources

(defstruct (source (:constructor %make-source (name drain any-thread wake)))
  name drain any-thread wake)

(defvar *sources* '() "Every source, in the order added.")
(defvar *waited* nil "The source an idle owner is waiting in (when *WAKE* says 2).")

(defun add-owner-source (name drain &key any-thread wake)
  "Have every pump drain another source of events: (FUNCALL DRAIN TIMEOUT) takes what has arrived
   and handles it, true if it did anything.  On the owner TIMEOUT is normally 0, and the source's
   producer calls (OWNER-WAKE NAME) after queuing input, so an owner idle in a pump wakes.

   WAKE, if given, is a function of no arguments any thread may call that makes a DRAIN now
   waiting return soon (it sends the source's channel a message DRAIN ignores).  A source with a
   WAKE is where an idle owner WAITS -- (FUNCALL DRAIN TIMEOUT) -- instead of on its own inbox:
   the source's input then wakes the owner directly, and messages to the owner (OWNER-CALL,
   OWNER-POST, work results) call WAKE.  So DRAIN must wait up to TIMEOUT when given one.

   ANY-THREAD: on a Lisp whose threads share memory a pump off the owner calls DRAIN too, with
   that pump's TIMEOUT.  DRAIN and WAKE are best symbols.  NAME (an EQUAL key) replaces a source
   of that name.  Call it on the owner."
  (remove-owner-source name)
  (setf *sources* (append *sources* (list (%make-source name drain any-thread wake))))
  name)

(defun remove-owner-source (name)
  (setf *sources* (remove name *sources* :key #'source-name :test #'equal))
  nil)

(defun owner-wake (&optional source)
  "From any thread, after queuing input for the source named SOURCE: wake the owner if it is idle
   in PUMP (unless it is waiting in that very source).  Cheap when it is not idle (one read)."
  (let ((f (svref *wake* 0)))
    (cond ((eql f 1)
           (setf (svref *wake* 0) 0)
           (let ((in *inbox*))
             (when in (ignore-errors (bobbin:channel-send in (list :wake) :timeout 0)))))
          ((eql f 2)
           (let ((w *waited*))
             (when (and w (not (equal (source-name w) source)))
               (setf (svref *wake* 0) 0)
               (ignore-errors (funcall (source-wake w))))))))
  nil)

(defun %wake-for-inbox ()
  "After sending to the owner's inbox: an owner waiting in a source would not see it -- wake it."
  (when (eql (svref *wake* 0) 2)
    (let ((w *waited*))
      (when w
        (setf (svref *wake* 0) 0)
        (ignore-errors (funcall (source-wake w)))))))

(defun %shared-memory-p ()
  "Do this Lisp's threads share memory (so a thread may touch what the owner made)?  Not on modus."
  (not (bobbin::%modus-channels-p)))

;;; ---------------------------------------------------------------------------
;;; Owner locks

(define-condition owner-busy (condition)
  ((key :initarg :key :reader owner-busy-key))
  (:report (lambda (c s) (format s "~a is held by a task suspended in a wait" (owner-busy-key c))))
  (:documentation "An OWNER-LOCK another task holds.  Not an ERROR: handlers for errors do not
   swallow it, so it reaches the lane or timer runner, which runs the work again later."))

(defvar *owner-locks* (make-hash-table :test #'equal) "key -> (task . depth)")

(defun owner-lock-held-p (key) (and (gethash key *owner-locks*) t))

(defun %owner-lock-acquire (key)
  (let ((h (gethash key *owner-locks*)))
    (cond ((null h) (setf (gethash key *owner-locks*) (cons *task* 1)) t)
          ((eq (car h) *task*) (incf (cdr h)) t)
          (t nil))))

(defun %owner-lock-release (key)
  (let ((h (gethash key *owner-locks*)))
    (when h (if (> (cdr h) 1) (decf (cdr h)) (remhash key *owner-locks*)))))

(defmacro with-owner-lock ((key &key (busy '(error 'owner-busy :key k))) &body body)
  "Run BODY holding the owner lock KEY (compared with EQUAL).  Re-entrant for this task; held by
   another (suspended) task, evaluate BUSY instead (default: signal OWNER-BUSY, with K bound to
   the key)."
  (let ((g (gensym "KEY")))
    `(let ((,g ,key))
       (if (%owner-lock-acquire ,g)
           (unwind-protect (progn ,@body) (%owner-lock-release ,g))
           (let ((k ,g)) (declare (ignorable k)) ,busy)))))

(defun owner-lock (key)
  "Try to take KEY for this task: T, or NIL when another task holds it.  (WITH-OWNER-LOCK releases
   what it takes; a bare OWNER-LOCK is held until the task's code releases it.)"
  (%owner-lock-acquire key))

;;; ---------------------------------------------------------------------------
;;; Lanes and timers (made and run on the owner only)

(defstruct (lane (:constructor %make-lane))
  name
  (items '())                    ; queued work, first to run first; the owner may reorder it
  (run nil)                      ; (lambda (item)): how an item is run (default: FUNCALL it)
  (ready nil)                    ; (lambda ()): NIL while the lane must not start
  (busy nil) (deferred-until 0) (live t) (close-when-empty nil)
  (runs 0))

(defvar *lanes* '() "Every live lane.")
(defvar *timers* '() "Every live timer.")

(defun make-lane (&key name run ready close-when-empty)
  "A lane: its items run one at a time, in order, on the owner when it pumps.  RUN is called with
   each item (default FUNCALL); READY, when given, holds the lane while it answers NIL.
   CLOSE-WHEN-EMPTY: the lane closes itself once it has run its last item."
  (let ((l (%make-lane :name name :run run :ready ready :close-when-empty close-when-empty)))
    (setf *lanes* (append *lanes* (list l)))
    l))

(defun lane-push (lane item) (setf (lane-items lane) (append (lane-items lane) (list item))) item)
(defun lane-push-front (lane item) (push item (lane-items lane)) item)
(defun lane-close (lane)
  "Stop LANE: nothing more of it runs (an item running now finishes)."
  (setf (lane-live lane) nil (lane-items lane) '())
  (setf *lanes* (remove lane *lanes*))
  lane)
(defun lane-depth (lane) (length (lane-items lane)))
(defun lane-busy-p (lane) (lane-busy lane))
(defun lane-live-p (lane) (and lane (lane-live lane)))
(defun lane-idle-p (lane) (and (not (lane-busy lane)) (null (lane-items lane))))

(defstruct (timer (:constructor %make-timer))
  name fn interval (due 0) (running nil) (live t) (once nil) (runs 0))

(defun every-seconds (interval fn &key name (first interval))
  "Run FN on the owner every INTERVAL seconds, measured from the end of its last run (as a thread
   doing (loop (sleep interval) (fn)) did), the first time FIRST seconds from now.  Never two runs
   of one timer at once; an error in FN is reported, not raised.  Returns the timer (CANCEL-TIMER)."
  (let ((tm (%make-timer :name name :fn fn :interval interval :due (+ (%now) first))))
    (push tm *timers*)
    tm))

(defun after-seconds (seconds fn &key name)
  "Run FN on the owner once, SECONDS from now.  Returns the timer."
  (let ((tm (%make-timer :name name :fn fn :interval 0 :due (+ (%now) seconds) :once t)))
    (push tm *timers*)
    tm))

(defun cancel-timer (tm)
  (when tm (setf (timer-live tm) nil) (setf *timers* (remove tm *timers*)))
  tm)
(defun timer-live-p (tm) (and tm (timer-live tm)))

(defun %run-timer (tm)
  (setf (timer-running tm) t)
  (let ((*task* tm) (again nil))
    (unwind-protect
         (handler-case (funcall (timer-fn tm))
           (owner-busy () (setf again t))           ; run again soon
           (error (e) (%warn "timer ~a: ~a" (timer-name tm) e)))
      (setf (timer-running tm) nil)
      (incf (timer-runs tm))
      (cond ((and (timer-once tm) (not again)) (cancel-timer tm))
            (t (setf (timer-due tm)
                     (+ (%now) (if again (max 0.05 (min 1 (timer-interval tm))) (timer-interval tm))))))))
  t)

(defun %run-lane (lane)
  (let ((item (pop (lane-items lane))) (*task* lane))
    (setf (lane-busy lane) t)
    (let ((done nil))
      (unwind-protect
           (handler-case (progn (if (lane-run lane) (funcall (lane-run lane) item) (funcall item))
                                (setf done t))
             (owner-busy ()
               ;; refused for a lock a suspended task holds: first in line again, a little later
               (when (lane-live lane)
                 (push item (lane-items lane))
                 (setf (lane-deferred-until lane) (+ (%now) 0.05))))
             (error (e) (setf done t) (%warn "lane ~a: ~a" (lane-name lane) e)))
        (setf (lane-busy lane) nil)
        (when done (incf (lane-runs lane)))
        (when (and (lane-close-when-empty lane) (null (lane-items lane))) (lane-close lane)))))
  t)

(defun task-name (&optional (task *task*))
  "What TASK (default: the one running now) is called: a lane's or timer's name, \"owner\" for the
   owner's own code."
  (cond ((lane-p task) (or (lane-name task) "lane"))
        ((timer-p task) (or (timer-name task) "timer"))
        ((consp task) (format nil "~(~{~a~^ ~}~)" task))
        (t "owner")))

;;; ---------------------------------------------------------------------------
;;; Messages from other threads: OWNER-CALL and OWNER-POST in, RUN-WORK results back

(defvar *work-results* (make-hash-table) "ticket -> (status . value), owner-side.")

(defun %handle-message (m)
  (case (first m)
    (:wake)
    (:call (destructuring-bind (slot tag fname args) (rest m)
             (let ((reply (let ((*task* (list :call fname)))
                            (handler-case (list :ok tag (apply fname args))
                              (owner-busy (c) (list :busy tag (princ-to-string c)))
                              (serious-condition (e) (list :error tag (princ-to-string e)))))))
               (unless (ignore-errors (bobbin:channel-send (svref *call-replies* slot) reply :timeout 5))
                 (%warn "could not answer an owner-call of ~a" fname)))))
    (:post (destructuring-bind (fname args) (rest m)
             (let ((*task* (list :post fname)))
               (handler-case (apply fname args)
                 (serious-condition (e) (%warn "owner-post of ~a: ~a" fname e))))))
    (:work (destructuring-bind (ticket status value) (rest m)
             (setf (gethash ticket *work-results*) (cons status value))))
    (t (%warn "unknown message ~s" (first m)))))

(define-condition owner-call-error (error)
  ((message :initarg :message :reader owner-call-error-message))
  (:report (lambda (c s) (write-string (owner-call-error-message c) s))))

(defun owner-post (fname args &key (timeout 60))
  "Have the owner run (APPLY FNAME ARGS) the next time it pumps, and return at once (T; NIL if the
   owner's inbox stayed full for TIMEOUT s).  FNAME a symbol, ARGS plain data.  On the owner it is
   a plain call.  An error in it is reported on the owner, not raised."
  (cond ((owner-p) (apply fname args) t)
        ((null *inbox*) (error 'owner-call-error :message "no owner is running (bobbin.owner:start-owner)"))
        (t (prog1 (bobbin:channel-send *inbox* (list :post fname args) :timeout timeout)
             (%wake-for-inbox)))))

(defun owner-call (fname args &key (timeout 60))
  "Run (APPLY FNAME ARGS) on the owner, the next time it pumps, and return its value.  For a thread
   that is NOT the owner (on the owner it is a plain call).  FNAME is a symbol (a function made on
   another thread cannot cross on modus); ARGS and the value are plain data (they are copied).  An
   error there, or no answer within TIMEOUT seconds, signals OWNER-CALL-ERROR here.  A call refused
   for an OWNER-LOCK is sent again."
  (when (owner-p) (return-from owner-call (apply fname args)))
  (unless *inbox* (error 'owner-call-error :message "no owner is running (bobbin.owner:start-owner)"))
  (multiple-value-bind (slot ok) (bobbin:channel-receive *call-free* :timeout timeout)
    (unless ok (error 'owner-call-error :message "every owner-call slot is busy"))
    (unwind-protect
         (let ((reply-chan (svref *call-replies* slot))
               (deadline (%deadline timeout)))
           (loop
             (let ((tag (get-internal-real-time)))
               (bobbin:channel-send *inbox* (list :call slot tag fname args))
               (%wake-for-inbox)
               (loop
                 (multiple-value-bind (r got)
                     (bobbin:channel-receive reply-chan :timeout (max 0 (%left deadline)))
                   (cond ((not got)
                          (error 'owner-call-error
                                 :message (format nil "the owner did not run ~a within ~ds" fname timeout)))
                         ((not (eql (second r) tag)))   ; a late answer to an earlier, timed-out call
                         ((eq (first r) :ok) (return-from owner-call (third r)))
                         ((eq (first r) :busy)
                          (when (<= (%left deadline) 0)
                            (error 'owner-call-error :message (third r)))
                          (sleep 0.05) (return))          ; ask again
                         (t (error 'owner-call-error :message (third r)))))))))
      (bobbin:channel-send *call-free* slot))))

;;; Work threads: blocking calls off the owner.  A fixed pool, started on first use by the owner:
;;; a work thread is started on a named function and channels, so it holds nothing of the owner's.
;;; A work function is a symbol; its arguments and value plain data.

(defparameter *work-threads* 3 "How many work threads RUN-WORK starts (modus has 15 thread slots in all).")
(defvar *work-jobs* nil "CHANNEL: (ticket fname args) to the work threads.")
(defvar *work-pool* '())
(defvar *work-ticket* 0)
(defvar *work-pending* 0 "RUN-WORK calls waiting now (owner-side).")
(defvar *work-local* nil "On a work thread: that thread's own table (WORK-LOCAL).")

(defun work-local (key make)
  "On a work thread, that thread's own value for KEY, made by (FUNCALL MAKE) the first time (a
   connection pool kept across calls).  NIL elsewhere."
  (when *work-local*
    (multiple-value-bind (v found) (gethash key *work-local*)
      (if found v (setf (gethash key *work-local*) (funcall make))))))

(defun %work-loop (jobs inbox)
  (let ((*work-local* (make-hash-table :test #'equal)))   ; bound here: this thread's own
    (loop (multiple-value-bind (job ok) (bobbin:channel-receive jobs)
            (when ok
              (destructuring-bind (ticket fname args) job
                (let ((reply (handler-case (list :work ticket :ok (apply fname args))
                               (serious-condition (e) (list :work ticket :error (princ-to-string e))))))
                  (handler-case (bobbin:channel-send inbox reply :timeout 60)
                    (error ()   ; a value that cannot travel
                      (ignore-errors (bobbin:channel-send inbox (list :work ticket :error "unsendable value")
                                                          :timeout 60))))
                  (%wake-for-inbox))))))))

(define-condition work-error (error)
  ((message :initarg :message :reader work-error-message))
  (:report (lambda (c s) (write-string (work-error-message c) s))))

(defun run-work (fname args &key (timeout 120))
  "(APPLY FNAME ARGS) on a work thread, pumping meanwhile; its value.  Off the owner it is a plain
   call.  An error there signals WORK-ERROR here (its message), as does no answer within TIMEOUT
   seconds."
  (unless (owner-p) (return-from run-work (apply fname args)))
  (unless *work-jobs*
    (setf *work-jobs* (bobbin:make-channel :capacity *owner-message-bytes* :name "bobbin work")))
  (when (null *work-pool*)
    (setf *work-pool* (loop repeat *work-threads*
                            collect (sb-thread:make-thread #'%work-loop :name "bobbin work"
                                                                        :arguments (list *work-jobs* *inbox*)))))
  (let ((ticket (incf *work-ticket*)) (*work-pending* (1+ *work-pending*)))
    (bobbin:channel-send *work-jobs* (list ticket fname args))
    (unless (wait-until (lambda () (nth-value 1 (gethash ticket *work-results*))) :timeout timeout :step 0.2)
      (error 'work-error :message (format nil "~a did not answer within ~ds" fname timeout)))
    (destructuring-bind (status . value) (gethash ticket *work-results*)
      (remhash ticket *work-results*)
      (if (eq status :ok) value (error 'work-error :message value)))))

(defun work-quiet-p () "No RUN-WORK is waiting." (zerop *work-pending*))

;;; ---------------------------------------------------------------------------
;;; The pump

(defconstant +idle-cap+ 0.05
  "With sources, the owner waits on its inbox no longer than this at a time: a wake lost to a
   weakly ordered machine costs at most this much latency, never a hang.")

(defun %next-due ()
  (let ((due nil))
    (dolist (tm *timers*)
      (unless (timer-running tm) (setf due (if due (min due (timer-due tm)) (timer-due tm)))))
    (dolist (l *lanes*)
      (when (and (lane-items l) (not (lane-busy l)) (> (lane-deferred-until l) 0))
        (setf due (if due (min due (lane-deferred-until l)) (lane-deferred-until l)))))
    due))

(defun %runnable-lane-p (l now)
  (and (lane-live l) (lane-items l) (not (lane-busy l)) (<= (lane-deferred-until l) now)
       (or (null (lane-ready l)) (funcall (lane-ready l)))))

(defun %drain-sources ()
  (let ((any nil))
    (dolist (s *sources* any)
      (when (funcall (source-drain s) 0) (setf any t)))))

(defun %drain-inbox ()
  (let ((any nil) (in *inbox*))
    (when in
      (loop (multiple-value-bind (m ok) (bobbin:channel-try-receive in)
              (unless ok (return))
              (unless (eq (first m) :wake) (setf any t))
              (%handle-message m))))
    any))

(defun %owner-pump (timeout)
  (let ((*depth* (1+ *depth*)) (any nil))
    (flet ((drain ()
             (when (%drain-inbox) (setf any t))
             (when (and *sources* (%drain-sources)) (setf any t))))
      (drain)
      (when (< *depth* +max-depth+)
        (let ((now (%now)))
          (dolist (tm (copy-list *timers*))
            (when (and (timer-live tm) (not (timer-running tm)) (<= (timer-due tm) now))
              (setf any t) (%run-timer tm) (drain)))
          (let ((ls (copy-list *lanes*)))
            ;; rotate, so no lane always goes first
            (when ls (setf *lanes* (append (rest *lanes*) (list (first *lanes*)))))
            (dolist (l ls)
              (when (%runnable-lane-p l (%now))
                (setf any t) (%run-lane l) (drain))))))
      (when (and (not any) (plusp timeout))
        (let* ((due (%next-due))
               (wait (if due (max 0 (min timeout (- due (%now)))) timeout)))
          (when *sources* (setf wait (min wait +idle-cap+)))
          (cond ((null *inbox*) (when (plusp wait) (sleep wait)))
                ((plusp wait)
                 ;; Idle.  Say so (OWNER-WAKE rings), then look once more at what might have
                 ;; arrived before the flag was set -- that rang nobody -- and only then wait: in
                 ;; the first source that can be woken, else on the inbox.
                 (let ((src (find-if #'source-wake *sources*)))
                   (setf *waited* src
                         (svref *wake* 0) (if src 2 1))
                   (unwind-protect
                        (cond ((and src (%drain-inbox)) (setf any t))
                              ((and *sources* (%drain-sources)) (setf any t))
                              (src (when (funcall (source-drain src) wait) (setf any t)))
                              (t (multiple-value-bind (m ok) (bobbin:channel-receive *inbox* :timeout wait)
                                   (when ok
                                     (unless (eq (first m) :wake) (setf any t))
                                     (%handle-message m)))))
                     (setf (svref *wake* 0) 0)))
                 (drain)))))
      any)))

(defun %foreign-pump (timeout)
  (let ((src (and (%shared-memory-p) (find-if #'source-any-thread *sources*))))
    (cond (src
           (let ((any (funcall (source-drain src) timeout)))
             (dolist (s *sources* any)
               (when (and (not (eq s src)) (source-any-thread s) (funcall (source-drain s) 0))
                 (setf any t)))))
          (t (when (plusp timeout) (sleep timeout)) nil))))

(defun pump (&optional (timeout 0))
  "On the owner: deliver what other threads sent, drain every source, run the timers that are due
   and one item of every lane that can start; if none of that did anything, wait up to TIMEOUT
   seconds for something to.  T if anything ran.  Off the owner it waits TIMEOUT (draining the
   :ANY-THREAD sources meanwhile on a Lisp whose threads share memory)."
  (if (owner-p) (%owner-pump timeout) (%foreign-pump timeout)))

(defun pause-for (seconds)
  "Wait SECONDS, pumping meanwhile.  Use this, not SLEEP, wherever the owner waits for a peer or
   for time."
  (let ((deadline (%deadline seconds)))
    (loop (let ((left (%left deadline)))
            (when (<= left 0) (return))
            (pump (min left 0.05))))))

(defun wait-until (predicate &key (timeout 5) (step 0.05))
  "Call PREDICATE until it is true or TIMEOUT seconds pass, pumping between tries (at most STEP
   seconds at a time; on the owner it looks again as soon as a pump has done anything); its last
   value."
  (let ((deadline (%deadline timeout)))
    (loop (let ((v (funcall predicate)))
            (when (or v (<= (%left deadline) 0)) (return v)))
          (let ((slice (max 0 (min step (%left deadline)))))
            (if (owner-p)
                (unless (pump 0) (pump slice))
                (pump slice))))))

(defun pump-until (predicate &key (timeout 5) (slice 0.05))
  "Pump until PREDICATE is true -- looked at after every pump, so it returns as soon as the input
   that makes it true has been handled -- or TIMEOUT seconds pass; its last value."
  (let ((deadline (%deadline timeout)))
    (loop (let ((v (funcall predicate)))
            (when v (return v))
            (let ((left (%left deadline)))
              (when (<= left 0) (return (funcall predicate)))
              (pump (min left slice)))))))
