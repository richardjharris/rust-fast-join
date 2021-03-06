# rjoin - rico's join

Similar to unix join(1) or gjoin, with the following differences:

 - cross-joins (many-to-many joins) are not handled properly
   (will be detected at some point)

 - however, memory usage for cross joins is no longer O(n) which
   ensures constant memory usage and runtime

   for example, if one file has a huge number of rows mapping to
   a single row in the other file, they will not be buffered in
   memory.

 - join on multiple fields

 - distinction between empty (present but zero-width) and missing fields.
   placeholder value for missing fields can be supplied for both left
   and right sides, which simplifies creation of 'switching' data

 - unsure about UTF-8 support

 - no --check-order, --ignore-case, header handling
