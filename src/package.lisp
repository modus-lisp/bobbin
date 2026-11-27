;;;; src/package.lisp -- BOBBIN: the org's one portable concurrency mechanism.

(defpackage #:bobbin
  (:use #:cl)
  (:export
   ;; what is running underneath
   #:backend #:max-actors #:*report-errors*
   ;; channels (src/channel.lisp)
   #:channel #:channel-p #:make-channel #:channel-send #:channel-receive
   #:channel-try-receive #:channel-drain
   ;; actors (src/actor.lisp)
   #:spawn #:spawn-link #:spawn-monitor #:send #:receive #:receive-if #:self
   #:actor-alive-p #:link #:unlink #:monitor #:demonitor #:stop
   #:mailbox-full #:not-an-actor))
