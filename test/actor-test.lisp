;;;; test/actor-test.lisp -- actors: ordering, timeouts, selective receive, copies, refusals,
;;;; links and monitors, many actors, and a ping-pong throughput number.
;;;; Every actor here runs a NAMED function, so the same file runs on every backend.

(in-package #:bobbin.test)

(defun a-echo ()
  "Answer (from x) with (:echo x) until :stop."
  (loop (let ((m (receive)))
          (when (eq m :stop) (return))
          (send (first m) (list :echo (second m))))))

(defun a-collect (n reply-to)
  "Receive N messages, then send them back in the order they arrived."
  (send reply-to (list :got (loop repeat n collect (receive :timeout 10)))))

(defun a-waiter (reply-to secs)
  (let ((t0 (now)))
    (multiple-value-bind (m ok) (receive :timeout secs)
      (send reply-to (list :waited ok m (- (now) t0))))))

(defun a-selector (reply-to)
  (let ((r (receive-if (lambda (m) (and (consp m) (eq (first m) :reply))) :timeout 5)))
    (send reply-to (list :selected r (receive :timeout 1) (receive :timeout 1)
                         (multiple-value-list (receive :timeout 0.05))))))

(defun a-mutate-check (reply-to)
  "Receive a list, report it, keep it, receive :again, report the kept one again."
  (let ((v (receive :timeout 5)))
    (send reply-to (list :first v))
    (receive :timeout 5)
    (send reply-to (list :again v))))

(defun a-quick (x) x)
(defun a-crash (x) (error "crash ~a" x))
(defun a-wait-then-end () (receive :timeout 10))
(defun a-hold (reply-to) (send reply-to (list :up (self))) (receive :timeout 30))

(defun a-spawn-child (reply-to)
  "From inside an actor: a symbol may be spawned, a closure may not."
  (let ((child (spawn-monitor 'a-quick 1))
        (refused (handler-case (progn (spawn (lambda () 1)) nil) (error () t))))
    (send reply-to (list :child (receive :timeout 10) refused (self) child))))

(defun a-not-plain (reply-to)
  (send reply-to
        (list :refusals
              (handler-case (progn (send reply-to (lambda () 1)) nil) (error () t))
              (handler-case (progn (send reply-to (make-pt :x 1)) nil) (error () t))
              (handler-case (progn (send reply-to (make-instance 'thing)) nil) (error () t)))))

(defun a-pong ()
  (loop (let ((m (receive)))
          (when (eq m :stop) (return))
          (send (car m) (cdr m)))))

(defun a-pinger (pong n reply-to)
  (let ((me (self)) (t0 (now)))
    (dotimes (i n) (send pong (cons me i)) (receive))
    (send reply-to (list :pinged n (- (now) t0)))))

(defun recv-match (pred &optional (secs 10))
  (receive-if pred :timeout secs))

(defun exit-of (a &optional (secs 10))
  (recv-match (lambda (m) (and (consp m) (eq (first m) :exit) (eql (second m) a))) secs))

(defun %getenv-int (name default)
  (let ((v (bobbin::%getenv name)))
    (or (and v (ignore-errors (parse-integer v))) default)))

(defun run-actors ()
  (format t "~&-- actors (~s)~%" (backend))
  (let ((me (self)))
    (check "SELF on the main thread is a handle" (and (integerp me) (actor-alive-p me)))

    ;; ordering, round trip, copies
    (let ((e (spawn 'a-echo)))
      (check "spawn returns a live handle" (actor-alive-p e))
      (let ((v (list 1 "two" :three #\4 (expt 2 100) 1/3 2.5d0 #(5 6)
                     (make-array 2 :element-type '(unsigned-byte 8) :initial-contents '(7 8)))))
        (send e (list me v))
        (multiple-value-bind (r ok) (receive :timeout 10)
          (check "a message goes there and back" (and ok (equalp r (list :echo v))) r)
          (check "...as a copy" (and ok (not (eq (second r) v))))))
      (send e :stop)
      (check "an actor that returns ends (:exit ... :normal reaches a monitor)"
             (progn (monitor e) (equal (exit-of e) (list :exit e :normal)))))

    (let ((c (spawn 'a-collect 200 me)))
      (dotimes (i 200) (send c i))
      (check "200 messages arrive in the order they were sent"
             (equal (receive :timeout 20) (list :got (loop for i below 200 collect i)))))

    ;; copy semantics: mutate after sending
    (let ((a (spawn 'a-mutate-check me)) (v (list 1 (list 2 3) (copy-seq "abc"))))
      (send a v)
      (setf (car v) :changed (car (second v)) :changed (char (third v) 0) #\X)
      (let ((r1 (receive :timeout 10)))
        (send a :again)
        (let ((r2 (receive :timeout 10)))
          (check "a sent list mutated after SEND arrives unchanged"
                 (equal r1 (list :first (list 1 (list 2 3) "abc"))) r1)
          (check "...and the receiver's copy is still its own after the sender's changes"
                 (equal r2 (list :again (list 1 (list 2 3) "abc"))) r2))))

    ;; timeouts
    (let ((t0 (now)))
      (multiple-value-bind (m ok) (receive :timeout 0.2)
        (let ((dt (- (now) t0)))
          (check "main's receive times out with NIL NIL"
                 (and (null m) (null ok) (>= dt 0.15) (< dt 2)) dt))))
    (let ((w (spawn 'a-waiter me 0.2)))
      (declare (ignore w))
      (let ((r (receive :timeout 10)))
        (check "an actor's receive times out" (and (null (second r)) (>= (fourth r) 0.15)
                                                   (< (fourth r) 2)) r)))
    (let ((w (spawn 'a-waiter me 5)))
      (sleep 0.05)
      (send w :hello)
      (let ((r (receive :timeout 10)))
        (check "a message before the timeout wins" (and (second r) (eq (third r) :hello)
                                                        (< (fourth r) 4)) r)))
    (check "try-style receive (timeout 0) on an empty mailbox is NIL NIL"
           (equal (multiple-value-list (receive :timeout 0)) '(nil nil)))

    ;; selective receive
    (let ((s (spawn 'a-selector me)))
      (send s (list :other 1)) (send s (list :reply 7)) (send s (list :other 2))
      (check "selective receive takes the match and keeps the rest in order"
             (equal (receive :timeout 10)
                    (list :selected (list :reply 7) (list :other 1) (list :other 2) (list nil nil)))))
    (send me (list :b 1)) (send me (list :a 2)) (send me (list :b 3))
    (check "selective receive on main"
           (and (equal (recv-match (lambda (m) (eq (first m) :a)) 1) (list :a 2))
                (equal (receive :timeout 1) (list :b 1)) (equal (receive :timeout 1) (list :b 3))))
    (check "selective receive times out" (null (recv-match (lambda (m) (eq m :never)) 0.1)))

    ;; refusals
    (let ((e (spawn 'a-echo)))
      (check "a closure cannot be sent"
             (handler-case (progn (send e (lambda () 1)) nil) (error () t)))
      (check "a structure instance cannot be sent"
             (handler-case (progn (send e (make-pt :x 1)) nil) (error () t)))
      (check "a CLOS instance cannot be sent"
             (handler-case (progn (send e (make-instance 'thing :a 1)) nil) (error () t)))
      (check "spawn arguments are refused the same way"
             (handler-case (progn (spawn 'a-quick (make-pt)) nil) (error () t)))
      (send e :stop))
    (spawn 'a-not-plain me)
    (check "...and inside an actor too" (equal (receive :timeout 10) (list :refusals t t t)))
    (check "an unknown function is refused at SPAWN"
           (handler-case (progn (spawn 'no-such-function-anywhere) nil) (error () t)))

    ;; main may spawn a closure; an actor may spawn a symbol but not a closure
    (let* ((k 41) (a (spawn-monitor (lambda (x) (send me (list :closure (+ x k)))) 1)))
      (check "the main thread may spawn a closure" (equal (receive :timeout 10) (list :closure 42)))
      (exit-of a))
    (let ((p (spawn 'a-spawn-child me)))
      (let ((r (receive :timeout 15)))
        (check "an actor spawns (and monitors) a symbol, and is refused a closure"
               (and (eq (first r) :child) (equal (second r) (list :exit (fifth r) :normal))
                    (eq (third r) t))
               r))
      (exit-of p))

    ;; links and monitors
    (let ((a (spawn-monitor 'a-quick 5)))
      (check "monitor: normal end" (equal (exit-of a) (list :exit a :normal)))
      (check "a handle of an ended actor is not alive" (not (actor-alive-p a)))
      (check "SEND to an ended actor returns NIL" (null (send a :hello)))
      (monitor a)
      (check "monitor of an ended actor: :noproc at once" (equal (exit-of a 2) (list :exit a :noproc))))
    (let ((old *report-errors*))
      (setf *report-errors* nil)
      (let ((a (spawn-link 'a-crash 7)))
        (let ((x (exit-of a)))
          (check "link: an error ends the actor and its reason says so"
                 (and (eq (first (third x)) :error) (search "crash 7" (third (third x)))) x)))
      (let ((a (spawn 'a-wait-then-end)))
        (link a)
        (send a :go)
        (check "LINK after SPAWN: normal end reaches main" (equal (exit-of a) (list :exit a :normal))))
      (let ((a (spawn 'a-wait-then-end)))
        (monitor a) (demonitor a) (send a :go)
        (check "after DEMONITOR no exit message comes" (null (exit-of a 0.5))))
      (setf *report-errors* old))

    ;; many actors, and slots reused
    (let* ((n (min (max-actors) (%getenv-int "BOBBIN_TEST_MANY" 1000)))
           (t0 (now))
           (as (loop repeat n collect (spawn 'a-hold me)))
           (ups (loop repeat n collect (receive :timeout 30)))
           (t1 (now)))
      (check (format nil "~d actors at once (the most this backend allows is ~d)" n (max-actors))
             (and (= n (count :up ups :key #'first)) (every #'actor-alive-p as)))
      (format t "     ~d spawned and answered in ~,3f s~%" n (- t1 t0))
      (check "one more than the most is refused"
             (or (> (max-actors) n)
                 (handler-case (progn (spawn 'a-quick 1) nil) (error () t))))
      (dolist (a as) (monitor a) (send a :done))
      (check "...and all of them end" (every (lambda (a) (exit-of a 30)) as))
      (let* ((bs (loop repeat n collect (spawn 'a-hold me)))
             (ups2 (loop repeat n collect (receive :timeout 30))))
        (check "their slots are reused, and old handles stay dead"
               (and (= n (count :up ups2 :key #'first)) (notany #'actor-alive-p as)
                    (every #'actor-alive-p bs)))
        (dolist (b bs) (monitor b) (send b :done))
        (every (lambda (b) (exit-of b 30)) bs)))

    ;; ping-pong throughput
    (let* ((n (%getenv-int "BOBBIN_TEST_PINGS" 5000))
           (p (spawn 'a-pong)))
      (dotimes (i 50) (send p (cons me i)) (receive :timeout 5)) ; warm up (modus JIT)
      (let ((t0 (now)))
        (dotimes (i n) (send p (cons me i)) (receive :timeout 10))
        (let ((dt (- (now) t0)))
          (check "main <-> actor ping-pong" t)
          (format t "     PING-PONG main<->actor: ~d round trips in ~,3f s = ~,0f round trips/s~%"
                  n dt (/ n (max dt 1/1000)))))
      (spawn 'a-pinger p n me)
      (let ((r (receive :timeout 120)))
        (check "actor <-> actor ping-pong" (eq (first r) :pinged) r)
        (when (eq (first r) :pinged)
          (format t "     PING-PONG actor<->actor: ~d round trips in ~,3f s = ~,0f round trips/s~%"
                  n (third r) (/ n (max (third r) 1/1000)))))
      (send p :stop))))
