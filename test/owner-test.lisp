;;;; test/owner-test.lisp -- the owner loop: pump, lanes, timers, owner locks, OWNER-CALL /
;;;; OWNER-POST from other threads, RUN-WORK, and a second event source drained by the same pump.
;;;; Every other thread here runs a NAMED function and reports on a channel made on the main
;;;; thread, so the file runs on every Lisp and backend.

(defpackage #:bobbin.owner-test
  (:use #:cl)
  (:local-nicknames (#:own #:bobbin.owner) (#:bob #:bobbin))
  (:import-from #:bobbin.test #:check #:now))
(in-package #:bobbin.owner-test)

(defvar *log* '() "Owner-side record of what ran, newest first.")
(defvar *ran-on-owner* nil)
(defun note (x) (push x *log*) x)

;;; functions other threads ask the owner to run
(defun o-add (a b) (list :sum (+ a b) (own:owner-p)))
(defun o-fail () (error "deliberate failure"))
(defun o-post (x) (setf *ran-on-owner* (list x (own:owner-p))))
(defun o-locked (key) (own:with-owner-lock (key) (list :got key)))

;;; other threads
(defun t-owner-p (out) (bob:channel-send out (list :owner-p (own:owner-p))))
(defun t-caller (out)
  (bob:channel-send
   out (list (handler-case (own:owner-call 'o-add '(40 2)) (error (e) (list :err (princ-to-string e))))
             (handler-case (own:owner-call 'o-fail '())
               (own:owner-call-error (e) (list :owner-call-error (own:owner-call-error-message e)))))))
(defun t-poster (out)
  (bob:channel-send out (list :posted (own:owner-post 'o-post '(7)))))
(defun t-locked-caller (key out)
  (let ((t0 (now)))
    (bob:channel-send out (list (handler-case (own:owner-call 'o-locked (list key) :timeout 20)
                                  (error (e) (list :err (princ-to-string e))))
                                (- (now) t0)))))

;;; work functions (they run on work threads)
(defun w-slow (secs x) (sleep secs) (list :slept x))
(defun w-fail () (error "work failed"))
(defun w-count () (let ((cell (own:work-local :counter (lambda () (list 0))))) (incf (car cell))))

;;; a second event source: a producer thread feeds a channel made on main; the owner drains it
(defvar *src-events* nil "CHANNEL: producer -> owner.")
(defvar *src-go* nil "CHANNEL: owner -> producer.")
(defvar *src-seen* 0 "Owner-side: events the source has delivered.")
(defvar *src-drains-off-owner* 0)

(defun src-drain (timeout)
  (unless (own:owner-p) (incf *src-drains-off-owner*))
  (let ((any nil))
    (multiple-value-bind (m ok) (bob:channel-receive *src-events* :timeout timeout)
      (loop while ok
            do (unless (eq m :wake) (setf any t) (incf *src-seen*) (note (list :event m)))
               (multiple-value-setq (m ok) (bob:channel-try-receive *src-events*))))
    any))

(defun src-wake () (bob:channel-send *src-events* :wake :timeout 0))

(defun t-call-timed (out)
  (sleep 0.02)                          ; so the owner is idle, waiting, when the call comes
  (let ((t0 (now)))
    (bob:channel-send out (list (own:owner-call 'o-add '(1 1)) (- (now) t0)))))

(defun t-producer (name go events)
  "For each (:go n [delay]) from GO, wait DELAY s, send n events and ring the owner; :stop ends it."
  (loop (let ((m (bob:channel-receive go :timeout 30)))
          (when (or (null m) (eq m :stop)) (return))
          (when (third m) (sleep (third m)))
          (dotimes (i (second m)) (bob:channel-send events i))
          (own:owner-wake name))))

(defun t-foreign-pump (out)
  (let ((t0 (now)))
    (own:pump 0.2)
    (bob:channel-send out (list :pumped (- (now) t0)))))

(defun await (ch &optional (secs 20))
  "A reply from another thread, pumping meanwhile (the owner must keep pumping while it waits)."
  (let ((r nil) (got nil))
    (own:wait-until (lambda () (multiple-value-setq (r got) (bob:channel-try-receive ch)) got)
                    :timeout secs :step 0.01)
    (values r got)))

(defun spawn (fn &rest args)
  (sb-thread:make-thread fn :name "owner-test" :arguments args))

(defun run-owner ()
  (format t "~&-- the owner loop~%")
  (own:start-owner)
  (let ((out (bob:make-channel :name "owner-test out")))
    (check "the thread that started the owner is the owner" (own:owner-p))
    (spawn 't-owner-p out)
    (check "...and another thread is not" (equal (await out) '(:owner-p nil)))

    ;; the pump, idle
    (check "an idle pump does nothing" (null (own:pump 0)))
    (let ((t0 (now)))
      (own:pause-for 0.2)
      (let ((dt (- (now) t0)))
        (check "PAUSE-FOR waits as long as it is asked" (and (>= dt 0.19) (< dt 1)) dt)))
    (check "WAIT-UNTIL times out with the predicate's last value"
           (null (own:wait-until (lambda () nil) :timeout 0.1)))
    (check "WAIT-UNTIL returns the predicate's value" (eq (own:wait-until (lambda () :yes)) :yes))

    ;; OWNER-CALL and OWNER-POST
    (check "OWNER-CALL on the owner is a plain call" (equal (own:owner-call 'o-add '(1 2)) '(:sum 3 t)))
    (spawn 't-caller out)
    (let ((r (await out)))
      (check "OWNER-CALL from another thread runs on the owner and returns its value"
             (equal (first r) '(:sum 42 t)) r)
      (check "...and an error there is OWNER-CALL-ERROR in the caller"
             (and (eq (first (second r)) :owner-call-error) (search "deliberate" (second (second r)))) r))
    (setf *ran-on-owner* nil)
    (spawn 't-poster out)
    (let ((r (await out)))
      (own:wait-until (lambda () *ran-on-owner*) :timeout 10)
      (check "OWNER-POST returns at once and the owner runs it" (and (equal r '(:posted t))
                                                                     (equal *ran-on-owner* '(7 t)))
             (list r *ran-on-owner*)))

    ;; lanes: one item at a time per lane, in order; a waiting item lets the others run
    (setf *log* '())
    (let ((a (own:make-lane :name "a")) (b (own:make-lane :name "b")))
      (own:lane-push a (lambda () (note :a1) (own:pause-for 0.2) (note :a1-done)))
      (own:lane-push a (lambda () (note :a2)))
      (own:lane-push b (lambda () (note :b1)))
      (own:lane-push b (lambda () (note (list :b2 (own:task-name)))))
      (own:wait-until (lambda () (and (own:lane-idle-p a) (own:lane-idle-p b))) :timeout 10)
      (let ((log (reverse *log*)))
        (check "a lane runs its items in order, one at a time"
               (< (position :a1 log) (position :a1-done log) (position :a2 log)) log)
        (check "...and while one waits, another lane runs"
               (< (position :b1 log) (position :a1-done log)) log)
        (check "TASK-NAME is the lane's name" (member '(:b2 "b") log :test #'equal) log))
      (own:lane-close a) (own:lane-close b))

    ;; timers
    (let* ((n 0) (once 0)
           (tm (own:every-seconds 0.05 (lambda () (incf n)) :name "tick"))
           (t1 (own:after-seconds 0.1 (lambda () (incf once)))))
      (own:pause-for 0.5)
      (own:cancel-timer tm)
      (let ((m n))
        (own:pause-for 0.2)
        (check "EVERY-SECONDS runs on the owner as it pumps" (<= 4 m 11) m)
        (check "CANCEL-TIMER stops it" (= n m) (list m n)))
      (check "AFTER-SECONDS runs once" (and (= once 1) (not (own:timer-live-p t1))) once))
    (let* ((runs 0)
           (tm (own:every-seconds 0.02 (lambda () (incf runs) (error "timer trouble")) :name "bad")))
      (own:pause-for 0.1)
      (own:cancel-timer tm)
      (check "an error in a timer is reported and the timer keeps running" (> runs 1) runs))

    ;; owner locks
    (setf *log* '())
    (let ((a (own:make-lane :name "holder")) (b (own:make-lane :name "waiter")))
      ;; the holder takes the lock, then -- while it holds it and waits -- the other lane and
      ;; another thread's OWNER-CALL ask for it (a wait nests: they run inside its PAUSE-FOR)
      (own:lane-push a (lambda ()
                         (own:with-owner-lock (:k)
                           (own:with-owner-lock (:k) (note :reentered))
                           (note :held)
                           (own:lane-push b (lambda () (own:with-owner-lock (:k) (note :b-got-it))))
                           (spawn 't-locked-caller :k out)
                           (own:pause-for 0.3)
                           (note :released))))
      (let ((r (await out)))
        (own:wait-until (lambda () (and (own:lane-idle-p a) (own:lane-idle-p b))) :timeout 10)
        (let ((log (reverse *log*)))
          (check "an owner lock is re-entrant for the task holding it" (member :reentered log) log)
          (check "another lane is refused it (OWNER-BUSY) and runs again once it is free"
                 (< (position :held log) (position :released log) (position :b-got-it log)) log)
          (check "...and so is an OWNER-CALL, which is sent again"
                 (and (equal (first r) '(:got :k)) (>= (second r) 0.1)) r)
          (check "no lock is left held" (not (own:owner-lock-held-p :k)))))
      (own:lane-close a) (own:lane-close b))

    ;; RUN-WORK
    (let ((ticks 0))
      (let ((tm (own:every-seconds 0.02 (lambda () (incf ticks)))))
        (check "RUN-WORK returns the work thread's value" (equal (own:run-work 'w-slow '(0.3 9)) '(:slept 9)))
        (own:cancel-timer tm))
      (check "...and the owner pumps while it waits" (>= ticks 5) ticks))
    (check "an error on a work thread is WORK-ERROR here"
           (handler-case (progn (own:run-work 'w-fail '()) nil)
             (own:work-error (e) (search "work failed" (own:work-error-message e)))))
    (let ((counts (loop repeat 12 collect (own:run-work 'w-count '()))))
      (check "WORK-LOCAL keeps a work thread's own value across calls" (> (reduce #'max counts) 1) counts))
    (check "WORK-QUIET-P once every RUN-WORK has returned" (own:work-quiet-p))

    ;; a second event source, drained by the same pump
    (setf *src-events* (bob:make-channel :name "owner-test events")
          *src-go* (bob:make-channel :name "owner-test go")
          *src-seen* 0 *src-drains-off-owner* 0 *log* '())
    (own:add-owner-source :test-source 'src-drain :any-thread t)
    (spawn 't-producer :test-source *src-go* *src-events*)
    (bob:channel-send *src-go* (list :go 5))
    (check "PUMP-UNTIL delivers a source's events" (own:pump-until (lambda () (= *src-seen* 5)) :timeout 10)
           *src-seen*)
    ;; latency: the producer rings (OWNER-WAKE), so an idle owner does not sit out its wait (the
    ;; producer waits 5 ms first, so the owner is idle in its pump when the event comes)
    (let ((t0 (now)) (n 40))
      (dotimes (i n)
        (bob:channel-send *src-go* (list :go 1 0.005))
        (own:pump-until (lambda () (= *src-seen* (+ 5 i 1))) :timeout 10 :slice 1))
      (let ((ms (* 1000 (/ (- (now) t0) n))))
        (format t "     source round trip: ~,2f ms~%" ms)
        (check "OWNER-WAKE wakes an idle owner at once (5 ms + a round trip, well under the idle cap)"
               (and (= *src-seen* 45) (< ms 20)) (list *src-seen* ms))))
    ;; composition: a lane waiting in PAUSE-FOR gets the source's events delivered meanwhile
    (let ((lane (own:make-lane :name "composer")) (seen-in-lane nil))
      (own:lane-push lane (lambda ()
                            (let ((before *src-seen*))
                              (bob:channel-send *src-go* (list :go 3))
                              (own:wait-until (lambda () (>= *src-seen* (+ before 3))) :timeout 10)
                              (setf seen-in-lane (- *src-seen* before)))))
      (own:wait-until (lambda () seen-in-lane) :timeout 15)
      (own:lane-close lane)
      (check "a lane waiting in WAIT-UNTIL sees the source's events" (eql seen-in-lane 3) seen-in-lane))
    ;; off the owner: an :ANY-THREAD source is drained only where threads share memory
    (setf *src-drains-off-owner* 0)
    (spawn 't-foreign-pump out)
    (let ((r (await out)))
      (check "PUMP off the owner waits its timeout" (and (eq (first r) :pumped) (>= (second r) 0.15)) r))
    (if (own::%shared-memory-p)
        (check "...draining the :ANY-THREAD sources (threads share memory here)"
               (plusp *src-drains-off-owner*) *src-drains-off-owner*)
        (check "...and does not touch the owner's sources (modus)"
               (zerop *src-drains-off-owner*) *src-drains-off-owner*))
    ;; a source that can be woken: an idle owner waits IN it, and messages to the owner wake it
    (own:add-owner-source :test-source 'src-drain :wake 'src-wake)
    (let ((t0 (now)) (n 20) (base *src-seen*))
      (dotimes (i n)
        (bob:channel-send *src-go* (list :go 1 0.005))
        (own:pump-until (lambda () (= *src-seen* (+ base i 1))) :timeout 10 :slice 1))
      (let ((ms (* 1000 (/ (- (now) t0) n))))
        (format t "     waited-in source round trip: ~,2f ms~%" ms)
        (check "an owner idle in a source with WAKE gets its events at once"
               (and (= *src-seen* (+ base n)) (< ms 20)) (list *src-seen* ms))))
    (spawn 't-call-timed out)
    (multiple-value-bind (r got)
        (progn (let ((r nil) (got nil))
                 ;; pump with a long slice: only a WAKE ends the wait early
                 (own:pump-until (lambda () (multiple-value-setq (r got) (bob:channel-try-receive out)) got)
                                 :timeout 10 :slice 1)
                 (values r got)))
      (check "...and an OWNER-CALL wakes an owner waiting in it (well under the idle cap)"
             (and got (equal (first r) '(:sum 2 t)) (< (second r) 0.02)) r))
    (let ((t0 (now)))
      (own:run-work 'w-slow '(0.01 1))
      (check "...and so does a work thread's answer" (< (- (now) t0) 0.04) (- (now) t0)))
    (bob:channel-send *src-go* :stop)
    (own:remove-owner-source :test-source)
    (check "an idle pump does nothing, again" (null (own:pump 0)))))
