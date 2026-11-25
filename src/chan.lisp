;;;; src/chan.lisp -- CHAN: a message channel between threads that share no state.
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
;;;;   - on modus (#+modus-channel) a CHAN is a MODUS:CHANNEL;
;;;;   - elsewhere it is a lock, two condition variables and a queue of COPIES, bounded by an
;;;;     estimate of the bytes queued -- so a sender that runs ahead blocks (or times out) on SBCL
;;;;     just as it does on modus, and a receiver never shares structure with the sender.
;;;;
;;;; WHAT CAN BE SENT is what MODUS:CHANNEL-SEND takes, and the SBCL side refuses the rest the same
;;;; way (in the sender, before the channel is touched): NIL, T, numbers, characters, strings,
;;;; symbols, lists (dotted too, not circular), simple vectors, (unsigned-byte 8) vectors and hash
;;;; tables.  Values arrive EQUAL, never EQ.  (modus also carries DEFSTRUCT instances; the SBCL side
;;;; refuses them, so code that works here works there.)
;;;;
;;;; A CHAN IS MADE ONCE AND REUSED.  On modus a channel's ring, lock and wait queues are never
;;;; reclaimed (the synchronisation arena holds 65536 cells for the life of the process), so a
;;;; server makes its channels at start-up and hands them out, rather than making some per request.
;;;;
;;;; MAKE A CHAN ON THE THREAD THAT WILL HAND IT OUT, and pass it as an :ARGUMENTS element of
;;;; MAKE-THREAD: a channel made on the main thread may be named by every thread.

(defpackage #:cl-transport.chan
  (:use #:cl)
  (:export #:chan #:chan-p #:make-chan #:chan-send #:chan-receive #:chan-try-receive
           #:chan-drain))

(in-package #:cl-transport.chan)

#+modus-channel
(progn
  (deftype chan () 'modus:channel)
  (defun chan-p (x) (modus:channel-p x))
  (defun make-chan (&key (capacity 65536) name)
    "A channel of CAPACITY bytes (it bounds one message)."
    (modus:make-channel :capacity capacity :name name))
  (defun chan-send (ch value &key timeout)
    "Copy VALUE into CH: T once it is in, NIL if TIMEOUT seconds passed with CH too full."
    (modus:channel-send ch value :timeout timeout))
  (defun chan-receive (ch &key timeout)
    "The next value sent on CH, as a fresh copy, and T; NIL NIL if TIMEOUT passed first."
    (modus:channel-receive ch :timeout timeout)))

#-modus-channel
(progn
  (defstruct (chan (:constructor %make-chan (name capacity)))
    name capacity
    (used 0)                                ; estimated bytes queued
    (head '()) (tail '())                   ; queue of (value . size)
    (mutex (sb-thread:make-mutex :name "cl-transport chan"))
    (not-empty (sb-thread:make-waitqueue))
    (not-full (sb-thread:make-waitqueue)))

  (defun make-chan (&key (capacity 65536) name)
    "A channel of CAPACITY bytes (it bounds one message)."
    (when (or (not (integerp capacity)) (< capacity 64))
      (error "make-chan: :CAPACITY must be an integer of at least 64 bytes, not ~S." capacity))
    (%make-chan name capacity))

  (defun %refuse (x)
    (error "chan-send: ~S cannot be sent between threads (only plain data: numbers, characters, ~
            strings, symbols, lists, simple and octet vectors, hash tables)." x))

  (defun %copy (x depth)
    "A copy of X sharing nothing mutable with it, and its size estimate in bytes: (values copy size)."
    (when (> depth 10000) (error "chan-send: the value is nested too deeply (circular?)"))
    (typecase x
      ((or null (eql t) number character symbol) (values x 8))
      (string (values (copy-seq x) (+ 8 (length x))))
      ((vector (unsigned-byte 8))
       (values (make-array (length x) :element-type '(unsigned-byte 8) :initial-contents x)
               (+ 8 (length x))))
      (cons
       ;; Iterative over the spine, so a long list does not cost stack per element.
       (let ((head nil) (tail nil) (size 8) (count 0))
         (loop while (consp x)
               do (when (> (incf count) 10000000) (error "chan-send: the list is circular"))
                  (multiple-value-bind (c n) (%copy (car x) (1+ depth))
                    (incf size (+ 16 n))
                    (let ((cell (list c)))
                      (if tail (setf (cdr tail) cell) (setf head cell))
                      (setf tail cell)))
                  (setf x (cdr x)))
         (when x
           (multiple-value-bind (c n) (%copy x (1+ depth))
             (incf size n)
             (setf (cdr tail) c)))
         (values head size)))
      (simple-vector
       (let ((v (make-array (length x))) (size 8))
         (dotimes (i (length x))
           (multiple-value-bind (c n) (%copy (svref x i) (1+ depth))
             (setf (svref v i) c) (incf size (+ 8 n))))
         (values v size)))
      (hash-table
       (let ((h (make-hash-table :test (hash-table-test x) :size (max 1 (hash-table-count x))))
             (size 16))
         (maphash (lambda (k v)
                    (multiple-value-bind (ck nk) (%copy k (1+ depth))
                      (multiple-value-bind (cv nv) (%copy v (1+ depth))
                        (setf (gethash ck h) cv) (incf size (+ 16 nk nv)))))
                  x)
         (values h size)))
      (t (%refuse x))))

  (defun %wait (queue mutex timeout)
    ;; SBCL's CONDITION-WAIT counts re-acquiring MUTEX against TIMEOUT, and a timeout that falls
    ;; in that part returns NIL with MUTEX NOT HELD -- take it back before WITH-MUTEX releases it.
    (or (sb-thread:condition-wait queue mutex :timeout timeout)
        (unless (sb-thread:holding-mutex-p mutex) (sb-thread:grab-mutex mutex) nil)))

  (defun %remaining (deadline)
    (and deadline (max 0 (/ (- deadline (get-internal-real-time)) internal-time-units-per-second))))

  (defun %deadline (timeout)
    (and timeout (+ (get-internal-real-time) (round (* timeout internal-time-units-per-second)))))

  (defun chan-send (ch value &key timeout)
    "Copy VALUE into CH: T once it is in, NIL if TIMEOUT seconds passed with CH too full."
    (multiple-value-bind (copy size) (%copy value 0)
      (when (> size (chan-capacity ch))
        (error "chan-send: a ~D-byte message does not fit in a ~D-byte channel."
               size (chan-capacity ch)))
      (let ((deadline (%deadline timeout)))
        (sb-thread:with-mutex ((chan-mutex ch))
          (loop while (> (+ (chan-used ch) size) (chan-capacity ch))
                do (let ((left (%remaining deadline)))
                     (when (and left (<= left 0)) (return-from chan-send nil))
                     (%wait (chan-not-full ch) (chan-mutex ch) left)))
          (let ((cell (list (cons copy size))))
            (if (chan-tail ch) (setf (cdr (chan-tail ch)) cell) (setf (chan-head ch) cell))
            (setf (chan-tail ch) cell))
          (incf (chan-used ch) size)
          (sb-thread:condition-broadcast (chan-not-empty ch))
          t))))

  (defun chan-receive (ch &key timeout)
    "The next value sent on CH, as a fresh copy, and T; NIL NIL if TIMEOUT passed first."
    (let ((deadline (%deadline timeout)))
      (sb-thread:with-mutex ((chan-mutex ch))
        (loop until (chan-head ch)
              do (let ((left (%remaining deadline)))
                   (when (and left (<= left 0)) (return-from chan-receive (values nil nil)))
                   (%wait (chan-not-empty ch) (chan-mutex ch) left)))
        (let ((item (pop (chan-head ch))))
          (unless (chan-head ch) (setf (chan-tail ch) nil))
          (decf (chan-used ch) (cdr item))
          (sb-thread:condition-broadcast (chan-not-full ch))
          (values (car item) t))))))

(defun chan-try-receive (ch)
  "CHAN-RECEIVE without waiting: the next value and T, or NIL NIL."
  (chan-receive ch :timeout 0))

(defun chan-drain (ch)
  "Throw away everything queued on CH; the number of values dropped."
  (loop for n from 0 while (nth-value 1 (chan-try-receive ch)) finally (return n)))
