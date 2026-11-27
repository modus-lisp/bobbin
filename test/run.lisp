;;;; test/run.lisp -- bobbin's whole suite as a script; exits 0 iff every check passes.
;;;;   sbcl --script test/run.lisp                 (modus --script test/run.lisp too)
;;;;   BOBBIN_BACKEND=threads modus --script test/run.lisp   ; the threads backend on modus
(require :asdf)
(asdf:initialize-source-registry)
(push (make-pathname :name nil :type nil :defaults (merge-pathnames "../" (or *load-truename* "./")))
      asdf:*central-registry*)
(asdf:load-system "bobbin/test" :verbose nil)
(let ((ok (funcall (find-symbol "RUN" "BOBBIN.TEST"))))
  (funcall (find-symbol "STOP" "BOBBIN"))
  (finish-output)
  (sb-ext:exit :code (if ok 0 1) :abort t))
