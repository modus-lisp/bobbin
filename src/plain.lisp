;;;; src/plain.lisp -- what a message may be, and the copy that carries it.
;;;;
;;;; A message between threads or actors is PLAIN DATA, copied: NIL, T, numbers, characters,
;;;; strings, symbols, lists (dotted too, not circular), simple vectors, (unsigned-byte 8)
;;;; vectors and hash tables.  Everything else -- a closure, a structure or CLOS instance, a
;;;; condition, a stream, a channel -- is refused in the SENDER, before anything is queued, on
;;;; every backend.  modus's own channels and actors carry DEFSTRUCT instances too; bobbin
;;;; refuses them there as well, so that code which runs on one Lisp runs on the other.
;;;;
;;;; On SBCL %PLAIN-COPY makes the copy itself (the receiver must never share structure with
;;;; the sender, or a sharing bug would only show on modus).  On modus the runtime copies
;;;; (encode in the sender's heap, decode in the receiver's), so there it only checks and
;;;; measures.

(in-package #:bobbin)

(defun %refuse (who x)
  (error "~a: ~S cannot be sent between threads or actors (only plain data: numbers, ~
          characters, strings, symbols, lists, simple and octet vectors, hash tables)." who x))

(defun %plain-copy (x copy who &optional (depth 0))
  "(values copy size): a copy of X sharing nothing mutable with it when COPY is true, else X
   itself, and an estimate of its size in bytes.  Signals if X is not plain data."
  (when (> depth 10000) (error "~a: the value is nested too deeply (circular?)" who))
  (typecase x
    ((or null (eql t) number character symbol) (values x 8))
    (string (values (if copy (copy-seq x) x) (+ 8 (length x))))
    ((vector (unsigned-byte 8))
     (values (if copy
                 (make-array (length x) :element-type '(unsigned-byte 8) :initial-contents x)
                 x)
             (+ 8 (length x))))
    (cons
     ;; Iterative over the spine, so a long list does not cost stack per element.
     (let ((head nil) (tail nil) (size 8) (count 0) (l x))
       (loop while (consp l)
             do (when (> (incf count) 10000000) (error "~a: the list is circular" who))
                (multiple-value-bind (c n) (%plain-copy (car l) copy who (1+ depth))
                  (incf size (+ 16 n))
                  (when copy
                    (let ((cell (list c)))
                      (if tail (setf (cdr tail) cell) (setf head cell))
                      (setf tail cell))))
                (setf l (cdr l)))
       (when l
         (multiple-value-bind (c n) (%plain-copy l copy who (1+ depth))
           (incf size n)
           (when copy (setf (cdr tail) c))))
       (values (if copy head x) size)))
    (simple-vector
     (let ((v (if copy (make-array (length x)) x)) (size 8))
       (dotimes (i (length x))
         (multiple-value-bind (c n) (%plain-copy (svref x i) copy who (1+ depth))
           (when copy (setf (svref v i) c))
           (incf size (+ 8 n))))
       (values v size)))
    (hash-table
     (let ((h (and copy (make-hash-table :test (hash-table-test x)
                                          :size (max 1 (hash-table-count x)))))
           (size 16))
       (maphash (lambda (k v)
                  (multiple-value-bind (ck nk) (%plain-copy k copy who (1+ depth))
                    (multiple-value-bind (cv nv) (%plain-copy v copy who (1+ depth))
                      (when copy (setf (gethash ck h) cv))
                      (incf size (+ 16 nk nv)))))
                x)
       (values (if copy h x) size)))
    (t (%refuse who x))))
