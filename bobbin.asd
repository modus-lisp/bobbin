;;;; bobbin.asd -- the org's one portable concurrency mechanism: actors and channels.
;;;;
;;;; Actors (SPAWN / SEND / RECEIVE / RECEIVE-IF / SELF / LINK / MONITOR) and copying message
;;;; CHANNELs, on modus's own actor runtime and message channels where they exist and on native
;;;; threads with a copying mailbox elsewhere (SBCL).  Messages are plain data, copied, never
;;;; shared, on every Lisp.  No dependencies beyond the SB-THREAD surface SBCL and modus share.

(defsystem "bobbin"
  :description "Actors and copying message channels: modus's actor runtime and channels where they
                exist, native threads and a locked queue of copies elsewhere."
  :version "0.1.0"
  :author "ynniv"
  :license "MIT"
  :serial t
  :components ((:module "src" :serial t
                :components ((:file "package")
                             (:file "plain")      ; what a message may be; the copy
                             (:file "channel")    ; CHANNEL: modus channel, or a queue of copies
                             (:file "actor"))))   ; actors on modus's runtime or on threads
  :in-order-to ((test-op (test-op "bobbin/test"))))

(defsystem "bobbin/test"
  :description "bobbin's suite: sbcl --script test/run.lisp (modus --script too)."
  :depends-on ("bobbin")
  :serial t
  :components ((:module "test" :serial t
                :components ((:file "package")
                             (:file "channel-test")
                             (:file "actor-test"))))
  :perform (test-op (o c) (uiop:symbol-call '#:bobbin.test '#:run)))
