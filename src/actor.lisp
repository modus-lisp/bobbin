;;;; src/actor.lisp -- actors: SPAWN, SEND, RECEIVE, RECEIVE-IF, SELF, LINK, MONITOR.
;;;;
;;;; ONE SURFACE, THREE BACKENDS, chosen when this file is LOADED from what the Lisp has:
;;;;
;;;;   :MODUS-ACTORS  modus's M:N actor runtime (MODUS:ACTORS-*, the hosted x86-64 CLI): actors
;;;;                  are scheduled over a few native threads, each with its own GC region; a
;;;;                  blocked RECEIVE gives the thread back.  Started lazily, on the main thread,
;;;;                  by the first SPAWN / RECEIVE there (*ACTOR-RUNTIME-CONFIG*).
;;;;   :THREADS       one native thread per actor, and a mailbox that is a bobbin CHANNEL -- a
;;;;                  MODUS:CHANNEL on modus (every modus CLI with channels), the locked queue of
;;;;                  COPIES elsewhere (SBCL).  So on SBCL a message is copied too, and code that
;;;;                  shares structure through a message breaks on SBCL just as it would on modus.
;;;;
;;;; BOBBIN_BACKEND=threads in the environment when bobbin is loaded forces :THREADS (to test
;;;; that backend on a modus that has the actor runtime); BOBBIN_BACKEND=actors insists on the
;;;; runtime and signals if there is none.
;;;;
;;;; AN ACTOR HANDLE IS AN INTEGER (slot + generation), so it is itself plain data: put (SELF) in
;;;; a message to be answered.  A handle names one actor for ever: once that actor has ended,
;;;; its slot may be reused, but messages sent to the old handle are dropped, not delivered to
;;;; the new occupant (every message travels in an envelope carrying the generation).
;;;;
;;;; THE MAIN THREAD IS AN ACTOR TOO: SELF there is its handle, and it can RECEIVE.  Any other
;;;; thread that bobbin did not start may SEND, but SELF / RECEIVE there signal NOT-AN-ACTOR.
;;;;
;;;; WHAT AN ACTOR RUNS is a function and arguments.  The function is a SYMBOL naming a global
;;;; function (from anywhere), or a function object -- a closure -- only when SPAWN is called on
;;;; the MAIN thread: on modus a closure an actor or worker made cannot be handed to another
;;;; (the runtime refuses the store), so bobbin refuses it on every Lisp, in the caller.  The
;;;; arguments are plain data and are COPIED to the new actor, like a message.
;;;;
;;;; EXITS.  When an actor's function returns, or signals an error it does not handle, every
;;;; actor LINKed to it or MONITORing it is sent (:EXIT handle reason), REASON being :NORMAL or
;;;; (:ERROR condition-type "message").  LINK / MONITOR of an actor that has already ended sends
;;;; (:EXIT handle :NOPROC) at once.  A link is two monitors; an exit is only a message -- it
;;;; does not end the linked actor.
;;;;
;;;; STATE THAT IS SHARED is fixnums in vectors made here, at load time, on the main thread, and
;;;; written under one mutex: on modus an actor or worker may store a fixnum into main's memory
;;;; but not an object of its own, so nothing else is shared.  Each actor's own state (its handle,
;;;; the messages RECEIVE-IF set aside) is a dynamic binding made in the actor.

(in-package #:bobbin)

(define-condition not-an-actor (error)
  ((operation :initarg :operation :reader not-an-actor-operation))
  (:report (lambda (c s)
             (format s "bobbin:~(~a~): this thread is not a bobbin actor (only the main thread ~
                        and threads SPAWN started are)." (not-an-actor-operation c)))))

(define-condition mailbox-full (error)
  ((actor :initarg :actor :reader mailbox-full-actor))
  (:report (lambda (c s)
             (format s "bobbin:send: actor ~a has not drained its mailbox in time."
                     (mailbox-full-actor c)))))

(defvar *report-errors* t
  "When true, an actor ended by an unhandled error says so on *ERROR-OUTPUT*.  Set it on the main
   thread; the exit message carries the error either way.")

;;; ---- the backend ----------------------------------------------------------------------------

(defvar *modus-actors-start* (%modus-fn "ACTORS-START"))
(defvar *modus-actors-stop* (%modus-fn "ACTORS-STOP"))
(defvar *modus-actors-spawn* (%modus-fn "ACTORS-SPAWN"))
(defvar *modus-actors-send* (%modus-fn "ACTORS-SEND"))
(defvar *modus-actors-receive* (%modus-fn "ACTORS-RECEIVE"))

(defun %getenv (name)
  (let* ((p (find-package "SB-EXT"))
         (s (and p (find-symbol "POSIX-GETENV" p))))
    (and s (fboundp s) (funcall s name))))

(defvar *backend*
  (let ((runtime (and *modus-actors-start* *modus-actors-spawn* *modus-actors-send*
                      *modus-actors-receive* t))
        (asked (%getenv "BOBBIN_BACKEND")))
    (cond ((equalp asked "threads") :threads)
          ((equalp asked "actors")
           (if runtime :modus-actors
               (error "BOBBIN_BACKEND=actors, but this Lisp has no MODUS:ACTORS-START.")))
          ((and asked (plusp (length asked)))
           (error "BOBBIN_BACKEND must be \"threads\" or \"actors\", not ~S." asked))
          (runtime :modus-actors)
          (t :threads)))
  "Which backend this image runs actors on: :MODUS-ACTORS or :THREADS.  Fixed at load time.")

(defun backend () *backend*)

(defvar *actor-runtime-config* (list :threads 4 :slots 48 :region-bytes #x800000)
  "How the :MODUS-ACTORS backend starts modus's runtime (MODUS:ACTORS-START): scheduler threads,
   actor slots (at most 60), and each actor's GC semispace size in bytes -- an actor's live data
   must fit in one.  Set it on the main thread before the first SPAWN.")

(defconstant +slot-bits+ 13)
(defconstant +watchers+ 16 "How many actors may LINK to or MONITOR one actor at once.")

(defvar *mailbox-bytes* 262144
  "On :THREADS, the capacity in bytes of each actor's mailbox (it bounds one message, and what may
   be queued at once).  Fixed when bobbin is loaded on modus.")

(defun %modus-threads-p () (and (eq *backend* :threads) (%modus-channels-p)))

(defvar *nslots*
  (ecase *backend*
    (:modus-actors 62)                         ; modus actor ids 1..61 (1 = main)
    (:threads (if (%modus-channels-p) 16 4096)))
  "Size of the slot table.  On modus threads: the 16 thread slots, main's included.")

(defun %main-slot () (if (eq *backend* :modus-actors) 1 0))

(defvar *lock* (sb-thread:make-mutex :name "bobbin actors"))
(defvar *gen* (make-array *nslots* :initial-element 0))
(defvar *state* (make-array *nslots* :initial-element 0)) ; 0 free, 1 alive, 2 ended
(defvar *watch* (make-array (* *nslots* +watchers+) :initial-element -1))
(defvar *closures* (make-array *nslots* :initial-element nil))
(defvar *mailboxes*
  (let ((v (make-array *nslots* :initial-element nil)))
    ;; On modus every mailbox is made NOW, on the loading (main) thread, so any thread may name
    ;; it; on SBCL only main's is, and the rest as slots are first used.
    (when (eq *backend* :threads)
      (dotimes (i (if (%modus-channels-p) *nslots* 1))
        (setf (svref v i) (make-channel :capacity *mailbox-bytes* :name "bobbin mailbox"))))
    v))
(defvar *runtime-started* nil)

(setf (svref *state* (%main-slot)) 1)

(defvar *self* nil "This actor's handle; bound in each actor, NIL (main or nobody) globally.")
(defvar *deferred* nil "Messages RECEIVE-IF set aside, oldest first; bound in each actor.")

(declaim (inline %slot %gen %handle))
(defun %slot (h) (logand h (1- (ash 1 +slot-bits+))))
(defun %gen (h) (ash h (- +slot-bits+)))
(defun %handle (slot gen) (logior slot (ash gen +slot-bits+)))

(defun %on-main-p () (and (null *self*) (sb-thread:main-thread-p)))

(defun max-actors ()
  "How many actors may exist at once besides the main thread, on this backend."
  (ecase *backend*
    (:modus-actors (getf *actor-runtime-config* :slots))
    (:threads (1- *nslots*))))

(defun %ensure-runtime ()
  (when (and (eq *backend* :modus-actors) (not (eq *runtime-started* t)))
    (when (eq *runtime-started* :stopped)
      (error "bobbin: the actor runtime was stopped (BOBBIN:STOP); modus starts it once per process."))
    (unless (sb-thread:main-thread-p)
      (error "bobbin: the first SPAWN or RECEIVE must be on the main thread (it starts modus's ~
              actor runtime)."))
    (destructuring-bind (&key threads slots region-bytes) *actor-runtime-config*
      (funcall *modus-actors-start* threads slots region-bytes))
    (setf *runtime-started* t))
  t)

(defun self ()
  "This actor's handle (the main thread's too)."
  (cond (*self*)
        ((sb-thread:main-thread-p) (%handle (%main-slot) 0))
        (t (error 'not-an-actor :operation 'self))))

(defun actor-alive-p (actor)
  "Is ACTOR (a handle) still running?"
  (and (integerp actor) (>= actor 0)
       (let ((slot (%slot actor)))
         (and (< slot *nslots*)
              (eql (svref *state* slot) 1)
              (eql (svref *gen* slot) (%gen actor))))))

;;; ---- watchers (under *LOCK*) ----------------------------------------------------------------

(defun %watch-add (slot watcher)
  (let ((base (* slot +watchers+)))
    (flet ((try ()
             (dotimes (i +watchers+ nil)
               (when (eql (svref *watch* (+ base i)) watcher) (return t))
               (when (eql (svref *watch* (+ base i)) -1)
                 (setf (svref *watch* (+ base i)) watcher)
                 (return t)))))
      (or (try)
          ;; Full: drop watchers that have ended (a monitor that died first leaves its entry).
          (progn (dotimes (i +watchers+)
                   (unless (actor-alive-p (svref *watch* (+ base i)))
                     (setf (svref *watch* (+ base i)) -1)))
                 (try))
          (error "bobbin: actor slot ~D already has ~D watchers." slot +watchers+)))))

(defun %watch-remove (slot watcher)
  (let ((base (* slot +watchers+)))
    (dotimes (i +watchers+)
      (when (eql (svref *watch* (+ base i)) watcher) (setf (svref *watch* (+ base i)) -1)))))

(defun %watch-take (slot)
  (let ((base (* slot +watchers+)) (out '()))
    (dotimes (i +watchers+ out)
      (let ((w (svref *watch* (+ base i))))
        (unless (eql w -1) (push w out) (setf (svref *watch* (+ base i)) -1))))))

;;; ---- raw transport per backend --------------------------------------------------------------

(defun %raw-send (slot envelope timeout)
  "T once ENVELOPE is in SLOT's mailbox, NIL if TIMEOUT seconds passed."
  (ecase *backend*
    (:threads (channel-send (svref *mailboxes* slot) envelope :timeout timeout))
    (:modus-actors
     (%plain-copy envelope nil 'send)
     (if (member :modus-actor-send-timeout *features*)
         ;; the runtime waits TIMEOUT seconds for room and answers NIL after them
         (and (funcall *modus-actors-send* slot envelope :timeout timeout) t)
         ;; an older modus waits a fixed 10 s for room, then signals
         (handler-case (progn (funcall *modus-actors-send* slot envelope) t)
           (error (e)
             (if (search "has not drained" (princ-to-string e)) nil (error e))))))))

(defun %raw-receive (slot timeout)
  (ecase *backend*
    (:threads (channel-receive (svref *mailboxes* slot) :timeout timeout))
    (:modus-actors
     (funcall *modus-actors-receive* (and timeout (max 0 (ceiling (* timeout 1000))))))))

;;; ---- spawn ----------------------------------------------------------------------------------

(defun %check-function (fn)
  (cond ((symbolp fn)
         (unless (fboundp fn) (error "bobbin:spawn: ~S names no function." fn)))
        ((functionp fn)
         (unless (%on-main-p)
           (error "bobbin:spawn: a function object may only be spawned from the main thread; ~
                   from an actor or worker pass a SYMBOL naming a global function (on modus a ~
                   closure one thread made cannot be handed to another).")))
        (t (error "bobbin:spawn: ~S is not a function or a symbol." fn))))

(defun %claim-slot ()
  "Under *LOCK*: a free slot (marked alive, generation bumped) or NIL.  :THREADS only."
  (loop for i from 1 below *nslots*
        when (/= (svref *state* i) 1)
          do (setf (svref *state* i) 1)
             (setf (svref *gen* i) (1+ (svref *gen* i)))
             (%watch-take i)
             (return i)))

(defun %register (slot me link monitor)
  "Under *LOCK*: watchers of a fresh SLOT."
  (let ((child (%handle slot (svref *gen* slot))))
    (when (or link monitor) (%watch-add slot me))
    (when link (%watch-add (%slot me) child))
    child))

(defun %spawn (fn args link monitor)
  (%check-function fn)
  (%plain-copy args nil 'spawn)
  (%ensure-runtime)
  (let* ((me (and (or link monitor) (self)))
         (fnspec (if (symbolp fn) fn :closure))
         slot child)
    (ecase *backend*
      (:threads
       (sb-thread:with-mutex (*lock*)
         (setf slot (%claim-slot))
         (unless slot
           (error "bobbin:spawn: all ~D actors are running (the most this backend allows)."
                  (max-actors)))
         (unless (svref *mailboxes* slot)   ; SBCL: made on first use
           (setf (svref *mailboxes* slot) (make-channel :capacity *mailbox-bytes*
                                                        :name "bobbin mailbox")))
         (setf (svref *closures* slot) (if (symbolp fn) nil fn))
         (setf child (%register slot me link monitor)))
       (channel-drain (svref *mailboxes* slot))
       (channel-send (svref *mailboxes* slot) (list :%start (%gen child) fnspec args))
       (handler-case
           (sb-thread:make-thread #'%thread-entry :name "bobbin actor" :arguments (list slot))
         (error (e)
           (sb-thread:with-mutex (*lock*)
             (setf (svref *state* slot) 2 (svref *closures* slot) nil)
             (%watch-take slot)
             (when me (%watch-remove (%slot me) child)))
           (error "bobbin:spawn: no thread for the actor (~a); this backend allows ~D at once."
                  e (max-actors)))))
      (:modus-actors
       (setf slot (funcall *modus-actors-spawn* '%actor-entry))
       (sb-thread:with-mutex (*lock*)
         (setf (svref *state* slot) 1)
         (setf (svref *gen* slot) (1+ (svref *gen* slot)))
         (%watch-take slot)
         (setf (svref *closures* slot) (if (symbolp fn) nil fn))
         (setf child (%register slot me link monitor)))
       (funcall *modus-actors-send* slot (list :%start (%gen child) fnspec args))))
    child))

(defun spawn (function &rest arguments)
  "Start an actor running (APPLY FUNCTION ARGUMENTS); its handle.  FUNCTION is a symbol, or (on the
   main thread only) a function object; ARGUMENTS are plain data, copied."
  (%spawn function arguments nil nil))

(defun spawn-link (function &rest arguments)
  "SPAWN, and LINK the new actor to this one before it can end."
  (%spawn function arguments t nil))

(defun spawn-monitor (function &rest arguments)
  "SPAWN, and MONITOR the new actor from this one before it can end."
  (%spawn function arguments nil t))

;;; ---- the actor's own life -------------------------------------------------------------------

(defun %await-start (slot)
  (loop (multiple-value-bind (m ok) (%raw-receive slot nil)
          (when (and ok (consp m) (eq (car m) :%start)) (return (cdr m))))))

(defun %thread-entry (slot) (%run slot (%await-start slot)))
(defun %actor-entry () (let ((slot (funcall *modus-actors-self*))) (%run slot (%await-start slot))))

(defvar *modus-actors-self* (%modus-fn "ACTORS-SELF"))

(defun %run (slot start)
  (destructuring-bind (gen fnspec args) start
    (let* ((*self* (%handle slot gen))
           (*deferred* nil)
           (fn (if (eq fnspec :closure) (svref *closures* slot) fnspec))
           (reason :killed))
      (setf (svref *closures* slot) nil)
      (unwind-protect
           (handler-case (progn (apply fn args) (setf reason :normal))
             (error (e)
               (let ((text (or (ignore-errors (princ-to-string e)) "an error")))
                 (setf reason (list :error (type-of e) text))
                 (when *report-errors*
                   (ignore-errors
                    (format *error-output* "~&bobbin: actor ~D ended with an error: ~a~%"
                            *self* text)
                    (finish-output *error-output*))))))
        (%exit slot gen reason)))))

(defun %exit (slot gen reason)
  (let ((me (%handle slot gen)) (watchers '()))
    (sb-thread:with-mutex (*lock*)
      (when (eql (svref *gen* slot) gen)
        (setf watchers (%watch-take slot))
        ;; A link is two entries: take ours out of the other side's list too.
        (dolist (w watchers) (%watch-remove (%slot w) me))
        (setf (svref *state* slot) 2)))
    (dolist (w watchers)
      (ignore-errors (send w (list :exit me reason) :timeout 2)))))

;;; ---- messages -------------------------------------------------------------------------------

(defun send (actor message &key (timeout 10))
  "Copy MESSAGE (plain data) to ACTOR's mailbox: T once it is there, NIL if ACTOR has ended (the
   message is dropped).  Signals MAILBOX-FULL if the mailbox stays full for TIMEOUT seconds."
  (unless (and (integerp actor) (>= actor 0) (< (%slot actor) *nslots*))
    (error "bobbin:send: ~S is not an actor handle." actor))
  (cond ((not (actor-alive-p actor))
         (%plain-copy message nil 'send)            ; refused even when it would be dropped
         nil)
        ((%raw-send (%slot actor) (cons (%gen actor) message) timeout) t)
        (t (error 'mailbox-full :actor actor))))

(defun %my-slot-gen (operation)
  (let ((me (cond (*self*) ((sb-thread:main-thread-p) (%handle (%main-slot) 0))
                  (t (error 'not-an-actor :operation operation)))))
    (%ensure-runtime)
    (values (%slot me) (%gen me))))

(defun %next (slot gen deadline)
  "The next message for this actor generation from the mailbox: (values msg t) or (values nil nil)."
  (loop
    (multiple-value-bind (env ok) (%raw-receive slot (%remaining deadline))
      (cond ((not ok) (return (values nil nil)))
            ((and (consp env) (eql (car env) gen)) (return (values (cdr env) t)))))))

(defun receive (&key timeout)
  "The next message: (values message T), or (values NIL NIL) if TIMEOUT seconds pass first (NIL,
   the default, waits for ever).  Messages RECEIVE-IF set aside come first, in order."
  (multiple-value-bind (slot gen) (%my-slot-gen 'receive)
    (if *deferred*
        (values (pop *deferred*) t)
        (%next slot gen (%deadline timeout)))))

(defun receive-if (predicate &key timeout)
  "The first message satisfying PREDICATE (selective receive): (values message T), or (values NIL
   NIL) if TIMEOUT seconds pass first.  Messages that do not match are kept, in order, for later."
  (multiple-value-bind (slot gen) (%my-slot-gen 'receive-if)
    (let ((prev nil) (q *deferred*))
      (loop while q
            do (when (funcall predicate (car q))
                 (if prev (setf (cdr prev) (cdr q)) (setf *deferred* (cdr q)))
                 (return-from receive-if (values (car q) t)))
               (setf prev q q (cdr q))))
    (let ((deadline (%deadline timeout)))
      (loop
        (multiple-value-bind (m ok) (%next slot gen deadline)
          (unless ok (return (values nil nil)))
          (if (funcall predicate m)
              (return (values m t))
              (setf *deferred* (nconc *deferred* (list m)))))))))

;;; ---- links and monitors ---------------------------------------------------------------------

(defun %watch (actor both)
  (let ((me (self)) (gone nil))
    (sb-thread:with-mutex (*lock*)
      (if (actor-alive-p actor)
          (progn (%watch-add (%slot actor) me)
                 (when both (%watch-add (%slot me) actor)))
          (setf gone t)))
    (when gone (send me (list :exit actor :noproc)))
    actor))

(defun link (actor)
  "Link this actor and ACTOR: whichever ends first, the other is sent (:EXIT handle reason)."
  (%watch actor t))

(defun monitor (actor)
  "When ACTOR ends, send this actor (:EXIT actor reason)."
  (%watch actor nil))

(defun unlink (actor)
  (let ((me (self)))
    (sb-thread:with-mutex (*lock*)
      (when (actor-alive-p actor) (%watch-remove (%slot actor) me))
      (%watch-remove (%slot me) actor))
    actor))

(defun demonitor (actor)
  (let ((me (self)))
    (sb-thread:with-mutex (*lock*)
      (when (actor-alive-p actor) (%watch-remove (%slot actor) me)))
    actor))

(defun stop ()
  "Stop what bobbin started that can be stopped: on :MODUS-ACTORS, the runtime's scheduler threads
   (actors still blocked are abandoned with them).  On :THREADS, nothing -- actor threads end when
   their functions return."
  (when (and (eq *backend* :modus-actors) (eq *runtime-started* t))
    (funcall *modus-actors-stop*)
    (setf *runtime-started* :stopped))
  t)
