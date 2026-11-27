;;;; src/channel.lisp -- CHANNEL: a message channel between threads that share no state.
;;;;
;;;; WHY.  On modus a thread may not store an object it made into memory another thread made
;;;; (its region would be collected out from under the reference): the runtime refuses the store.
;;;; So the classic shapes -- a reader thread pushing datagrams onto a mailbox another thread
;;;; pops, a handler thread starting a session on a closure over objects it built -- fail there.
;;;; What does work is copying: MODUS:MAKE-CHANNEL encodes a value in the sender's heap, copies
;;;; the BYTES through a shared ring, and decodes a fresh value in the receiver's heap.
;;;;
;;;; THIS FILE IS THAT MODEL ON EVERY LISP, so a library has one threading design and not a
;;;; #+modus fork of it:
;;;;
;;;;   - where MODUS:MAKE-CHANNEL exists (modus's x86-64 and AArch64 CLIs) a CHANNEL is a
;;;;     MODUS:CHANNEL;
;;;;   - elsewhere it is a lock, two condition variables and a queue of COPIES, bounded by an
;;;;     estimate of the bytes queued -- so a sender that runs ahead blocks (or times out) on SBCL
;;;;     just as it does on modus, and a receiver never shares structure with the sender.
;;;;
;;;; Which one is decided when this file is LOADED, by asking whether the function exists, not
;;;; by reading #+sbcl / #+modus.
;;;;
;;;; WHAT CAN BE SENT is plain data (src/plain.lisp), refused the same way on every Lisp, in the
;;;; sender, before the channel is touched.  Values arrive EQUAL, never EQ.
;;;;
;;;; A CHANNEL IS MADE ONCE AND REUSED.  On modus a channel's ring, lock and wait queues are never
;;;; reclaimed (the synchronisation arena holds 65536 cells for the life of the process), so a
;;;; server makes its channels at start-up and hands them out, rather than making some per request.
;;;;
;;;; MAKE A CHANNEL ON THE THREAD THAT WILL HAND IT OUT, and pass it as an :ARGUMENTS element of
;;;; MAKE-THREAD (or keep it in a global): a channel made on the main thread may be named by every
;;;; thread.  A channel is not itself a message -- sending one copies its bytes, not the channel.

(in-package #:bobbin)

(defun %modus-fn (name)
  "The function MODUS:NAME if this Lisp has one, else NIL."
  (let* ((p (find-package "MODUS"))
         (s (and p (find-symbol name p))))
    (and s (fboundp s) (symbol-function s))))

(defvar *modus-make-channel* (%modus-fn "MAKE-CHANNEL"))
(defvar *modus-channel-send* (%modus-fn "CHANNEL-SEND"))
(defvar *modus-channel-receive* (%modus-fn "CHANNEL-RECEIVE"))
(defvar *modus-channel-p* (%modus-fn "CHANNEL-P"))

(defun %modus-channels-p () (and *modus-make-channel* t))

;;; ---- the queue of copies (every Lisp without MODUS:MAKE-CHANNEL) --------------------------

(defstruct (queue-channel (:constructor %make-queue-channel (name capacity)))
  name capacity
  (used 0)                                ; estimated bytes queued
  (head '()) (tail '())                   ; queue of (value . size)
  (mutex (sb-thread:make-mutex :name "bobbin channel"))
  (not-empty (sb-thread:make-waitqueue))
  (not-full (sb-thread:make-waitqueue)))

(deftype channel ()
  (if (%modus-channels-p)
      (find-symbol "CHANNEL" "MODUS")
      'queue-channel))

(defun %wait (queue mutex timeout)
  ;; SBCL's CONDITION-WAIT counts re-acquiring MUTEX against TIMEOUT, and a timeout that falls
  ;; in that part returns NIL with MUTEX NOT HELD -- take it back before WITH-MUTEX releases it.
  (or (sb-thread:condition-wait queue mutex :timeout timeout)
      (unless (sb-thread:holding-mutex-p mutex) (sb-thread:grab-mutex mutex) nil)))

(defun %remaining (deadline)
  (and deadline (max 0 (/ (- deadline (get-internal-real-time)) internal-time-units-per-second))))

(defun %deadline (timeout)
  (and timeout (+ (get-internal-real-time) (round (* timeout internal-time-units-per-second)))))

(defun %queue-send (ch value timeout)
  (multiple-value-bind (copy size) (%plain-copy value t 'channel-send)
    (when (> size (queue-channel-capacity ch))
      (error "channel-send: a ~D-byte message does not fit in a ~D-byte channel."
             size (queue-channel-capacity ch)))
    (let ((deadline (%deadline timeout)))
      (sb-thread:with-mutex ((queue-channel-mutex ch))
        (loop while (> (+ (queue-channel-used ch) size) (queue-channel-capacity ch))
              do (let ((left (%remaining deadline)))
                   (when (and left (<= left 0)) (return-from %queue-send nil))
                   (%wait (queue-channel-not-full ch) (queue-channel-mutex ch) left)))
        (let ((cell (list (cons copy size))))
          (if (queue-channel-tail ch)
              (setf (cdr (queue-channel-tail ch)) cell)
              (setf (queue-channel-head ch) cell))
          (setf (queue-channel-tail ch) cell))
        (incf (queue-channel-used ch) size)
        (sb-thread:condition-broadcast (queue-channel-not-empty ch))
        t))))

(defun %queue-receive (ch timeout)
  (let ((deadline (%deadline timeout)))
    (sb-thread:with-mutex ((queue-channel-mutex ch))
      (loop until (queue-channel-head ch)
            do (let ((left (%remaining deadline)))
                 (when (and left (<= left 0)) (return-from %queue-receive (values nil nil)))
                 (%wait (queue-channel-not-empty ch) (queue-channel-mutex ch) left)))
      (let ((item (pop (queue-channel-head ch))))
        (unless (queue-channel-head ch) (setf (queue-channel-tail ch) nil))
        (decf (queue-channel-used ch) (cdr item))
        (sb-thread:condition-broadcast (queue-channel-not-full ch))
        (values (car item) t)))))

;;; ---- the API ------------------------------------------------------------------------------

(defun make-channel (&key (capacity 65536) name)
  "A channel of CAPACITY bytes (it bounds one message, and what may be queued at once)."
  (when (or (not (integerp capacity)) (< capacity 64))
    (error "make-channel: :CAPACITY must be an integer of at least 64 bytes, not ~S." capacity))
  (if (%modus-channels-p)
      (funcall *modus-make-channel* :capacity capacity :name name)
      (%make-queue-channel name capacity)))

(defun channel-p (x)
  (if (%modus-channels-p)
      (and (funcall *modus-channel-p* x) t)
      (queue-channel-p x)))

(defun channel-send (ch value &key timeout)
  "Copy VALUE into CH: T once it is in, NIL if TIMEOUT seconds passed with CH too full.
   VALUE must be plain data (src/plain.lisp); anything else signals, here, in the sender."
  (if (%modus-channels-p)
      (progn (%plain-copy value nil 'channel-send)
             (and (funcall *modus-channel-send* ch value :timeout timeout) t))
      (%queue-send ch value timeout)))

(defun channel-receive (ch &key timeout)
  "The next value sent on CH, as a fresh copy, and T; NIL NIL if TIMEOUT seconds passed first
   (NIL waits forever)."
  (if (%modus-channels-p)
      (funcall *modus-channel-receive* ch :timeout timeout)
      (%queue-receive ch timeout)))

(defun channel-try-receive (ch)
  "CHANNEL-RECEIVE without waiting: the next value and T, or NIL NIL."
  (channel-receive ch :timeout 0))

(defun channel-drain (ch)
  "Throw away everything queued on CH; the number of values dropped."
  (loop for n from 0 while (nth-value 1 (channel-try-receive ch)) finally (return n)))
