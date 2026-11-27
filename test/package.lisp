;;;; test/package.lisp -- the suite's package and its one check function.

(defpackage #:bobbin.test
  (:use #:cl #:bobbin)
  (:export #:run #:run-channels #:run-actors))

(in-package #:bobbin.test)

(defvar *fails* 0)
(defvar *checks* 0)

(defun check (what ok &optional detail)
  (incf *checks*)
  (unless ok (incf *fails*))
  (format t "~a ~a~@[   ~s~]~%" (if ok "ok  " "FAIL") what (and (not ok) detail))
  (finish-output))

(defun now () (/ (get-internal-real-time) internal-time-units-per-second))

(defun run ()
  "Both suites; T iff every check passed."
  (setf *fails* 0 *checks* 0)
  (format t "~&bobbin on ~a ~a, backend ~s, at most ~d actors~%"
          (lisp-implementation-type) (lisp-implementation-version) (backend) (max-actors))
  (run-channels)
  (run-actors)
  (funcall (find-symbol "RUN-OWNER" "BOBBIN.OWNER-TEST"))
  (format t "~&~d/~d~%" (- *checks* *fails*) *checks*)
  (zerop *fails*))
