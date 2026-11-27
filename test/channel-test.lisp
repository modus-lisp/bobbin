;;;; test/channel-test.lisp -- CHANNEL: copies between threads that share no state.
;;;; Threads are started on NAMED functions with their channels as :ARGUMENTS -- the shape that
;;;; works on modus, where a worker may not be handed a closure it made, nor store its objects
;;;; into another thread's.

(in-package #:bobbin.test)

(defstruct pt x y)
(defclass thing () ((a :initarg :a)))

(defun echo-worker (in out)
  "Receive values until :STOP, send each back as (:echo value)."
  (loop (multiple-value-bind (v ok) (channel-receive in :timeout 10)
          (when (or (not ok) (eq v :stop)) (return))
          (channel-send out (list :echo v)))))

(defun sum-worker (in out n)
  "Receive N octet vectors, send back the sum of all their bytes."
  (let ((sum 0))
    (dotimes (i n) (let ((v (channel-receive in :timeout 10))) (incf sum (reduce #'+ v))))
    (channel-send out sum)))

(defun run-channels ()
  (format t "~&-- channels (~a)~%" (if (bobbin::%modus-channels-p) "MODUS:CHANNEL" "queue of copies"))
  (let ((in (make-channel :capacity 4096)) (out (make-channel :capacity 4096))
        (value (list 1 "two" :three #\4 (expt 2 100) 1/3 2.5d0 #(5 6) nil
                     (make-array 3 :element-type '(unsigned-byte 8) :initial-contents '(7 8 9))
                     '(dotted . pair))))
    (check "a channel is a CHANNEL" (and (channel-p in) (typep in 'channel)))
    (let ((th (sb-thread:make-thread #'echo-worker :name "echo" :arguments (list in out))))
      (channel-send in value)
      (multiple-value-bind (back ok) (channel-receive out :timeout 10)
        (check "a value goes there and back" (and ok (equalp back (list :echo value))) back)
        (check "...as a copy, not the same object" (and ok (not (eq (second back) value)))))
      (channel-send in :stop)
      (sb-thread:join-thread th :default nil)))

  (let ((ch (make-channel :capacity 256)))
    (check "receive on an empty channel times out with NIL NIL"
           (equal (multiple-value-list (channel-receive ch :timeout 0.1)) '(nil nil)))
    (check "try-receive on an empty channel is NIL NIL"
           (equal (multiple-value-list (channel-try-receive ch)) '(nil nil)))
    (check "a full channel makes a send with a timeout return NIL"
           (let ((v (make-array 100 :element-type '(unsigned-byte 8))))
             (and (channel-send ch v :timeout 1) (channel-send ch v :timeout 1)
                  (not (channel-send ch v :timeout 0.2)))))
    (check "drain throws everything queued away" (= 2 (channel-drain ch)))
    (check "a message larger than the channel signals"
           (handler-case (progn (channel-send ch (make-string 1000)) nil) (error () t)))
    (check "a closure cannot be sent"
           (handler-case (progn (channel-send ch (lambda () 1)) nil) (error () t)))
    (check "a structure instance cannot be sent (on modus too)"
           (handler-case (progn (channel-send ch (make-pt :x 1)) nil) (error () t)))
    (check "a CLOS instance cannot be sent"
           (handler-case (progn (channel-send ch (make-instance 'thing :a 1)) nil) (error () t)))
    (check "...and nothing refused was queued" (= 0 (channel-drain ch))))

  (let ((ch (make-channel :capacity 4096)) (v (list 1 (list 2 3) (copy-seq "abc"))))
    (channel-send ch v)
    (setf (car v) :changed (car (second v)) :changed (char (third v) 0) #\X)
    (check "mutating a value after sending it does not change what arrives"
           (equal (channel-receive ch :timeout 1) (list 1 (list 2 3) "abc"))))

  ;; a bounded channel carries more than its capacity in total: the sender blocks, the receiver drains
  (let ((in (make-channel :capacity 8192)) (out (make-channel :capacity 256)) (n 200))
    (let ((th (sb-thread:make-thread #'sum-worker :name "sum" :arguments (list in out n))))
      (dotimes (i n)
        (channel-send in (make-array 1000 :element-type '(unsigned-byte 8) :initial-element 1)))
      (check "200 KB through an 8 KB channel, all of it arrives"
             (eql (channel-receive out :timeout 30) (* n 1000)))
      (sb-thread:join-thread th :default nil))))
