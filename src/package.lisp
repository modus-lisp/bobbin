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

(defpackage #:bobbin.owner
  (:use #:cl)
  (:documentation "The OWNER LOOP (src/owner.lisp): one thread owns some state and runs everything
   that touches it when it pumps; other threads reach it by message.")
  (:export
   ;; the owner and its pump
   #:start-owner #:owner-p #:owner-thread
   #:pump #:pause-for #:wait-until #:pump-until #:pump-depth #:*task* #:task-name
   ;; other event sources drained by the same pump
   #:add-owner-source #:remove-owner-source #:owner-wake
   ;; lanes and timers
   #:lane #:make-lane #:lane-p #:lane-name #:lane-items #:lane-push #:lane-push-front #:lane-close
   #:lane-busy-p #:lane-idle-p #:lane-live-p #:lane-depth #:lane-runs
   #:timer #:timer-p #:timer-name #:every-seconds #:after-seconds #:cancel-timer #:timer-live-p
   #:timer-runs
   ;; locks between tasks on the owner
   #:owner-lock #:with-owner-lock #:owner-busy #:owner-busy-key #:owner-lock-held-p
   ;; from other threads
   #:owner-call #:owner-post #:owner-call-error #:owner-call-error-message
   #:*owner-call-slots* #:*owner-message-bytes*
   ;; work threads
   #:run-work #:work-error #:work-error-message #:work-local #:work-quiet-p #:*work-threads*))
