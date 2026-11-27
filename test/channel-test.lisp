;;;; test/chan-test.lisp -- CL-TRANSPORT.CHAN: copies between threads that share no state.
;;;;   sbcl --script test/chan-test.lisp        (modus --script too)
;;;; Threads are started on NAMED functions with their channels as :ARGUMENTS -- the shape that
;;;; works on modus, where a worker may not be handed a closure it made, nor store its objects
;;;; into another thread's.  Exits 0 iff all pass.
(require :asdf)
(asdf:initialize-source-registry)
(push (truename "./") asdf:*central-registry*)
(asdf:load-system "cl-transport/chan" :verbose nil)
(defpackage #:chan-test (:use #:cl #:cl-transport.chan))
(in-package #:chan-test)
(defvar *fails* 0) (defvar *checks* 0)
(defun check (what ok &optional detail)
  (incf *checks*) (unless ok (incf *fails*))
  (format t "~a ~a~@[   ~s~]~%" (if ok "ok  " "FAIL") what (and (not ok) detail)) (finish-output))

(defun echo-worker (in out)
  "Receive values until :STOP, send each back as (:echo value)."
  (loop (multiple-value-bind (v ok) (chan-receive in :timeout 10)
          (when (or (not ok) (eq v :stop)) (return))
          (chan-send out (list :echo v)))))

(defun sum-worker (in out n)
  "Receive N octet vectors, send back the sum of all their bytes."
  (let ((sum 0))
    (dotimes (i n) (let ((v (chan-receive in :timeout 10))) (incf sum (reduce #'+ v))))
    (chan-send out sum)))

(let ((in (make-chan :capacity 4096)) (out (make-chan :capacity 4096))
      (value (list 1 "two" :three #\4 (expt 2 100) 1/3 2.5d0 #(5 6) nil
                   (make-array 3 :element-type '(unsigned-byte 8) :initial-contents '(7 8 9))
                   '(dotted . pair))))
  (check "a channel is a CHAN" (chan-p in))
  (let ((th (sb-thread:make-thread #'echo-worker :name "echo" :arguments (list in out))))
    (chan-send in value)
    (multiple-value-bind (back ok) (chan-receive out :timeout 10)
      (check "a value goes there and back" (and ok (equalp back (list :echo value))) back)
      (check "...as a copy, not the same object" (and ok (not (eq (second back) value)))))
    (chan-send in :stop)
    (sb-thread:join-thread th :default nil)))

(let ((ch (make-chan :capacity 256)))
  (check "receive on an empty channel times out with NIL NIL"
         (equal (multiple-value-list (chan-receive ch :timeout 0.1)) '(nil nil)))
  (check "try-receive on an empty channel is NIL NIL"
         (equal (multiple-value-list (chan-try-receive ch)) '(nil nil)))
  (check "a full channel makes a send with a timeout return NIL"
         (let ((v (make-array 100 :element-type '(unsigned-byte 8))))
           (and (chan-send ch v :timeout 1) (chan-send ch v :timeout 1)
                (not (chan-send ch v :timeout 0.2)))))
  (check "drain throws everything queued away" (= 2 (chan-drain ch)))
  (check "a message larger than the channel signals"
         (handler-case (progn (chan-send ch (make-string 1000)) nil) (error () t)))
  (check "a closure cannot be sent"
         (handler-case (progn (chan-send ch (lambda () 1)) nil) (error () t))))

;; a bounded channel carries more than its capacity in total: the sender blocks, the receiver drains
(let ((in (make-chan :capacity 8192)) (out (make-chan :capacity 256)) (n 200))
  (let ((th (sb-thread:make-thread #'sum-worker :name "sum" :arguments (list in out n))))
    (dotimes (i n) (chan-send in (make-array 1000 :element-type '(unsigned-byte 8) :initial-element 1)))
    (check "200 KB through an 8 KB channel, all of it arrives" (eql (chan-receive out :timeout 30) (* n 1000)))
    (sb-thread:join-thread th :default nil)))

(format t "~d/~d~%" (- *checks* *fails*) *checks*)
(sb-ext:exit :code (if (zerop *fails*) 0 1) :abort t)
