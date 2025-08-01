///"use strict";
/*
* $Id: rawdeflate.js,v 0.3 2009/03/01 19:05:05 dankogai Exp dankogai $
*
* Original:
*   http://www.onicos.com/staff/iz/amuse/javascript/expert/deflate.txt
*/

// Zlib lib by djazz
//<JasobNoObfs> 
var Zlib = (function Zlib /* Name for debugging */() {
    //</JasobNoObfs>
    var Deflate = (function () {

        /* Copyright (C) 1999 Masanao Izumo <iz@onicos.co.jp>
        * Version: 1.0.1
        * LastModified: Dec 25 1999
        */

        /* constant parameters */
        var zip_WSIZE = 32768; 	// Sliding Window size
        var zip_STORED_BLOCK = 0;
        var zip_STATIC_TREES = 1;
        var zip_DYN_TREES = 2;

        /* for deflate */
        var zip_DEFAULT_LEVEL = 6;
        var zip_FULL_SEARCH = true;
        var zip_INBUFSIZ = 32768; // Input buffer size
        var zip_INBUF_EXTRA = 64; // Extra buffer
        var zip_OUTBUFSIZ = 1024 * 8;
        var zip_window_size = 2 * zip_WSIZE;
        var zip_MIN_MATCH = 3;
        var zip_MAX_MATCH = 258;
        var zip_BITS = 16;
        // for SMALL_MEM
        var zip_LIT_BUFSIZE = 0x2000;
        var zip_HASH_BITS = 13;
        // for MEDIUM_MEM
        // var zip_LIT_BUFSIZE = 0x4000;
        // var zip_HASH_BITS = 14;
        // for BIG_MEM
        // var zip_LIT_BUFSIZE = 0x8000;
        // var zip_HASH_BITS = 15;
        if (zip_LIT_BUFSIZE > zip_INBUFSIZ)
            alert("error: zip_INBUFSIZ is too small");
        if ((zip_WSIZE << 1) > (1 << zip_BITS))
            alert("error: zip_WSIZE is too large");
        if (zip_HASH_BITS > zip_BITS - 1)
            alert("error: zip_HASH_BITS is too large");
        if (zip_HASH_BITS < 8 || zip_MAX_MATCH != 258)
            alert("error: Code too clever");
        var zip_DIST_BUFSIZE = zip_LIT_BUFSIZE;
        var zip_HASH_SIZE = 1 << zip_HASH_BITS;
        var zip_HASH_MASK = zip_HASH_SIZE - 1;
        var zip_WMASK = zip_WSIZE - 1;
        var zip_NIL = 0; // Tail of hash chains
        var zip_TOO_FAR = 4096;
        var zip_MIN_LOOKAHEAD = zip_MAX_MATCH + zip_MIN_MATCH + 1;
        var zip_MAX_DIST = zip_WSIZE - zip_MIN_LOOKAHEAD;
        var zip_SMALLEST = 1;
        var zip_MAX_BITS = 15;
        var zip_MAX_BL_BITS = 7;
        var zip_LENGTH_CODES = 29;
        var zip_LITERALS = 256;
        var zip_END_BLOCK = 256;
        var zip_L_CODES = zip_LITERALS + 1 + zip_LENGTH_CODES;
        var zip_D_CODES = 30;
        var zip_BL_CODES = 19;
        var zip_REP_3_6 = 16;
        var zip_REPZ_3_10 = 17;
        var zip_REPZ_11_138 = 18;
        var zip_HEAP_SIZE = 2 * zip_L_CODES + 1;
        var zip_H_SHIFT = parseInt((zip_HASH_BITS + zip_MIN_MATCH - 1) /
			   zip_MIN_MATCH, 10);

        /* variables */
        var zip_free_queue;
        var zip_qhead, zip_qtail;
        var zip_initflag;
        var zip_outbuf = null;
        var zip_outcnt, zip_outoff;
        var zip_complete;
        var zip_window;
        var zip_d_buf;
        var zip_l_buf;
        var zip_prev;
        var zip_bi_buf;
        var zip_bi_valid;
        var zip_block_start;
        var zip_ins_h;
        var zip_hash_head;
        var zip_prev_match;
        var zip_match_available;
        var zip_match_length;
        var zip_prev_length;
        var zip_strstart;
        var zip_match_start;
        var zip_eofile;
        var zip_lookahead;
        var zip_max_chain_length;
        var zip_max_lazy_match;
        var zip_compr_level;
        var zip_good_match;
        var zip_nice_match;
        var zip_dyn_ltree;
        var zip_dyn_dtree;
        var zip_static_ltree;
        var zip_static_dtree;
        var zip_bl_tree;
        var zip_l_desc;
        var zip_d_desc;
        var zip_bl_desc;
        var zip_bl_count;
        var zip_heap;
        var zip_heap_len;
        var zip_heap_max;
        var zip_depth;
        var zip_length_code;
        var zip_dist_code;
        var zip_base_length;
        var zip_base_dist;
        var zip_flag_buf;
        var zip_last_lit;
        var zip_last_dist;
        var zip_last_flags;
        var zip_flags;
        var zip_flag_bit;
        var zip_opt_len;
        var zip_static_len;
        var zip_deflate_data;
        var zip_deflate_pos;

        /* objects (deflate) */

        var zip_DeflateCT = function () {
            this.fc = 0; // frequency count or bit string
            this.dl = 0; // father node in Huffman tree or length of bit string
        }

        var zip_DeflateTreeDesc = function () {
            this.dyn_tree = null; // the dynamic tree
            this.static_tree = null; // corresponding static tree or NULL
            this.extra_bits = null; // extra bits for each code or NULL
            this.extra_base = 0; // base index for extra_bits
            this.elems = 0; 	// max number of elements in the tree
            this.max_length = 0; // max bit length for the codes
            this.max_code = 0; 	// largest code with non zero frequency
        }

        /* Values for max_lazy_match, good_match and max_chain_length, depending on
        * the desired pack level (0..9). The values given below have been tuned to
        * exclude worst case performance for pathological files. Better values may be
        * found for specific files.
        */
        var zip_DeflateConfiguration = function (a, b, c, d) {
            this.good_length = a; // reduce lazy search above this match length
            this.max_lazy = b;    // do not perform lazy search above this match length
            this.nice_length = c; // quit search above this match length
            this.max_chain = d;
        }

        var zip_DeflateBuffer = function () {
            this.next = null;
            this.len = 0;
            this.ptr = new Array(zip_OUTBUFSIZ);
            this.off = 0;
        }

        /* constant tables */
        var zip_extra_lbits = [0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 0];
        var zip_extra_dbits = [0, 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 11, 11, 12, 12, 13, 13];
        var zip_extra_blbits = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 3, 7];
        var zip_bl_order = [16, 17, 18, 0, 8, 7, 9, 6, 10, 5, 11, 4, 12, 3, 13, 2, 14, 1, 15];
        var zip_configuration_table = [
	new zip_DeflateConfiguration(0, 0, 0, 0),
	new zip_DeflateConfiguration(4, 4, 8, 4),
	new zip_DeflateConfiguration(4, 5, 16, 8),
	new zip_DeflateConfiguration(4, 6, 32, 32),
	new zip_DeflateConfiguration(4, 4, 16, 16),
	new zip_DeflateConfiguration(8, 16, 32, 32),
	new zip_DeflateConfiguration(8, 16, 128, 128),
	new zip_DeflateConfiguration(8, 32, 128, 256),
	new zip_DeflateConfiguration(32, 128, 258, 1024),
	new zip_DeflateConfiguration(32, 258, 258, 4096)];


        /* routines (deflate) */

        var zip_deflate_start = function (level) {
            var i;

            if (!level)
                level = zip_DEFAULT_LEVEL;
            else if (level < 1)
                level = 1;
            else if (level > 9)
                level = 9;

            zip_compr_level = level;
            zip_initflag = false;
            zip_eofile = false;
            if (zip_outbuf != null)
                return;

            zip_free_queue = zip_qhead = zip_qtail = null;
            zip_outbuf = new Array(zip_OUTBUFSIZ);
            zip_window = new Array(zip_window_size);
            zip_d_buf = new Array(zip_DIST_BUFSIZE);
            zip_l_buf = new Array(zip_INBUFSIZ + zip_INBUF_EXTRA);
            zip_prev = new Array(1 << zip_BITS);
            zip_dyn_ltree = new Array(zip_HEAP_SIZE);
            for (i = 0; i < zip_HEAP_SIZE; i++)
                zip_dyn_ltree[i] = new zip_DeflateCT();
            zip_dyn_dtree = new Array(2 * zip_D_CODES + 1);
            for (i = 0; i < 2 * zip_D_CODES + 1; i++)
                zip_dyn_dtree[i] = new zip_DeflateCT();
            zip_static_ltree = new Array(zip_L_CODES + 2);
            for (i = 0; i < zip_L_CODES + 2; i++)
                zip_static_ltree[i] = new zip_DeflateCT();
            zip_static_dtree = new Array(zip_D_CODES);
            for (i = 0; i < zip_D_CODES; i++)
                zip_static_dtree[i] = new zip_DeflateCT();
            zip_bl_tree = new Array(2 * zip_BL_CODES + 1);
            for (i = 0; i < 2 * zip_BL_CODES + 1; i++)
                zip_bl_tree[i] = new zip_DeflateCT();
            zip_l_desc = new zip_DeflateTreeDesc();
            zip_d_desc = new zip_DeflateTreeDesc();
            zip_bl_desc = new zip_DeflateTreeDesc();
            zip_bl_count = new Array(zip_MAX_BITS + 1);
            zip_heap = new Array(2 * zip_L_CODES + 1);
            zip_depth = new Array(2 * zip_L_CODES + 1);
            zip_length_code = new Array(zip_MAX_MATCH - zip_MIN_MATCH + 1);
            zip_dist_code = new Array(512);
            zip_base_length = new Array(zip_LENGTH_CODES);
            zip_base_dist = new Array(zip_D_CODES);
            zip_flag_buf = new Array(parseInt(zip_LIT_BUFSIZE / 8, 10));
        }

        var zip_deflate_end = function () {
            zip_free_queue = zip_qhead = zip_qtail = null;
            zip_outbuf = null;
            zip_window = null;
            zip_d_buf = null;
            zip_l_buf = null;
            zip_prev = null;
            zip_dyn_ltree = null;
            zip_dyn_dtree = null;
            zip_static_ltree = null;
            zip_static_dtree = null;
            zip_bl_tree = null;
            zip_l_desc = null;
            zip_d_desc = null;
            zip_bl_desc = null;
            zip_bl_count = null;
            zip_heap = null;
            zip_depth = null;
            zip_length_code = null;
            zip_dist_code = null;
            zip_base_length = null;
            zip_base_dist = null;
            zip_flag_buf = null;
        }

        var zip_reuse_queue = function (p) {
            p.next = zip_free_queue;
            zip_free_queue = p;
        }

        var zip_new_queue = function () {
            var p;

            if (zip_free_queue != null) {
                p = zip_free_queue;
                zip_free_queue = zip_free_queue.next;
            }
            else
                p = new zip_DeflateBuffer();
            p.next = null;
            p.len = p.off = 0;

            return p;
        }

        var zip_head1 = function (i) {
            return zip_prev[zip_WSIZE + i];
        }

        var zip_head2 = function (i, val) {
            return zip_prev[zip_WSIZE + i] = val;
        }

        /* put_byte is used for the compressed output, put_ubyte for the
        * uncompressed output. However unlzw() uses window for its
        * suffix table instead of its output buffer, so it does not use put_ubyte
        * (to be cleaned up).
        */
        var zip_put_byte = function (c) {
            zip_outbuf[zip_outoff + zip_outcnt++] = c;
            if (zip_outoff + zip_outcnt == zip_OUTBUFSIZ)
                zip_qoutbuf();
        }

        /* Output a 16 bit value, lsb first */
        var zip_put_short = function (w) {
            w &= 0xffff;
            if (zip_outoff + zip_outcnt < zip_OUTBUFSIZ - 2) {
                zip_outbuf[zip_outoff + zip_outcnt++] = (w & 0xff);
                zip_outbuf[zip_outoff + zip_outcnt++] = (w >>> 8);
            } else {
                zip_put_byte(w & 0xff);
                zip_put_byte(w >>> 8);
            }
        }

        /* ==========================================================================
        * Insert string s in the dictionary and set match_head to the previous head
        * of the hash chain (the most recent string with same hash key). Return
        * the previous length of the hash chain.
        * IN  assertion: all calls to to INSERT_STRING are made with consecutive
        *    input characters and the first MIN_MATCH bytes of s are valid
        *    (except for the last MIN_MATCH-1 bytes of the input file).
        */
        var zip_INSERT_STRING = function () {
            zip_ins_h = ((zip_ins_h << zip_H_SHIFT)
		 ^ (zip_window[zip_strstart + zip_MIN_MATCH - 1] & 0xff))
	& zip_HASH_MASK;
            zip_hash_head = zip_head1(zip_ins_h);
            zip_prev[zip_strstart & zip_WMASK] = zip_hash_head;
            zip_head2(zip_ins_h, zip_strstart);
        }

        /* Send a code of the given tree. c and tree must not have side effects */
        var zip_SEND_CODE = function (c, tree) {
            zip_send_bits(tree[c].fc, tree[c].dl);
        }

        /* Mapping from a distance to a distance code. dist is the distance - 1 and
        * must not have side effects. dist_code[256] and dist_code[257] are never
        * used.
        */
        var zip_D_CODE = function (dist) {
            return (dist < 256 ? zip_dist_code[dist]
	    : zip_dist_code[256 + (dist >> 7)]) & 0xff;
        }

        /* ==========================================================================
        * Compares to subtrees, using the tree depth as tie breaker when
        * the subtrees have equal frequency. This minimizes the worst case length.
        */
        var zip_SMALLER = function (tree, n, m) {
            return tree[n].fc < tree[m].fc ||
      (tree[n].fc == tree[m].fc && zip_depth[n] <= zip_depth[m]);
        }

        /* ==========================================================================
        * <STRIKE>read string data</STRIKE>
        * read arraybuffer data
        */
        var zip_read_buff = function (buff, offset, n) {
            var i;
            var l = zip_deflate_data.length;
            for (i = 0; i < n && zip_deflate_pos < l; i += 1) {
                buff[offset + i] = zip_deflate_data[zip_deflate_pos++];
            }
            return i;
        }

        /* ==========================================================================
        * Initialize the "longest match" routines for a new file
        */
        var zip_lm_init = function () {
            var j;

            /* Initialize the hash table. */
            for (j = 0; j < zip_HASH_SIZE; j++)
                //	zip_head2(j, zip_NIL);
                zip_prev[zip_WSIZE + j] = 0;
            /* prev will be initialized on the fly */

            /* Set the default configuration parameters:
            */
            zip_max_lazy_match = zip_configuration_table[zip_compr_level].max_lazy;
            zip_good_match = zip_configuration_table[zip_compr_level].good_length;
            if (!zip_FULL_SEARCH)
                zip_nice_match = zip_configuration_table[zip_compr_level].nice_length;
            zip_max_chain_length = zip_configuration_table[zip_compr_level].max_chain;

            zip_strstart = 0;
            zip_block_start = 0;

            zip_lookahead = zip_read_buff(zip_window, 0, 2 * zip_WSIZE);
            if (zip_lookahead <= 0) {
                zip_eofile = true;
                zip_lookahead = 0;
                return;
            }
            zip_eofile = false;
            /* Make sure that we always have enough lookahead. This is important
            * if input comes from a device such as a tty.
            */
            while (zip_lookahead < zip_MIN_LOOKAHEAD && !zip_eofile)
                zip_fill_window();

            /* If lookahead < MIN_MATCH, ins_h is garbage, but this is
            * not important since only literal bytes will be emitted.
            */
            zip_ins_h = 0;
            for (j = 0; j < zip_MIN_MATCH - 1; j++) {
                //      UPDATE_HASH(ins_h, window[j]);
                zip_ins_h = ((zip_ins_h << zip_H_SHIFT) ^ (zip_window[j] & 0xff)) & zip_HASH_MASK;
            }
        }

        /* ==========================================================================
        * Set match_start to the longest match starting at the given string and
        * return its length. Matches shorter or equal to prev_length are discarded,
        * in which case the result is equal to prev_length and match_start is
        * garbage.
        * IN assertions: cur_match is the head of the hash chain for the current
        *   string (strstart) and its distance is <= MAX_DIST, and prev_length >= 1
        */
        var zip_longest_match = function (cur_match) {
            var chain_length = zip_max_chain_length; // max hash chain length
            var scanp = zip_strstart; // current string
            var matchp; 	// matched string
            var len; 	// length of current match
            var best_len = zip_prev_length; // best match length so far

            /* Stop when cur_match becomes <= limit. To simplify the code,
            * we prevent matches with the string of window index 0.
            */
            var limit = (zip_strstart > zip_MAX_DIST ? zip_strstart - zip_MAX_DIST : zip_NIL);

            var strendp = zip_strstart + zip_MAX_MATCH;
            var scan_end1 = zip_window[scanp + best_len - 1];
            var scan_end = zip_window[scanp + best_len];

            /* Do not waste too much time if we already have a good match: */
            if (zip_prev_length >= zip_good_match)
                chain_length >>= 2;

            //  Assert(encoder->strstart <= window_size-MIN_LOOKAHEAD, "insufficient lookahead");

            do {
                //    Assert(cur_match < encoder->strstart, "no future");
                matchp = cur_match;

                /* Skip to next match if the match length cannot increase
                * or if the match length is less than 2:
                */
                if (zip_window[matchp + best_len] != scan_end ||
	   zip_window[matchp + best_len - 1] != scan_end1 ||
	   zip_window[matchp] != zip_window[scanp] ||
	   zip_window[++matchp] != zip_window[scanp + 1]) {
                    continue;
                }

                /* The check at best_len-1 can be removed because it will be made
                * again later. (This heuristic is not always a win.)
                * It is not necessary to compare scan[2] and match[2] since they
                * are always equal when the other bytes match, given that
                * the hash keys are equal and that HASH_BITS >= 8.
                */
                scanp += 2;
                matchp++;

                /* We check for insufficient lookahead only every 8th comparison;
                * the 256th check will be made at strstart+258.
                */
                do {
                } while (zip_window[++scanp] == zip_window[++matchp] &&
		zip_window[++scanp] == zip_window[++matchp] &&
		zip_window[++scanp] == zip_window[++matchp] &&
		zip_window[++scanp] == zip_window[++matchp] &&
		zip_window[++scanp] == zip_window[++matchp] &&
		zip_window[++scanp] == zip_window[++matchp] &&
		zip_window[++scanp] == zip_window[++matchp] &&
		zip_window[++scanp] == zip_window[++matchp] &&
		scanp < strendp);

                len = zip_MAX_MATCH - (strendp - scanp);
                scanp = strendp - zip_MAX_MATCH;

                if (len > best_len) {
                    zip_match_start = cur_match;
                    best_len = len;
                    if (zip_FULL_SEARCH) {
                        if (len >= zip_MAX_MATCH) break;
                    } else {
                        if (len >= zip_nice_match) break;
                    }

                    scan_end1 = zip_window[scanp + best_len - 1];
                    scan_end = zip_window[scanp + best_len];
                }
            } while ((cur_match = zip_prev[cur_match & zip_WMASK]) > limit
	    && --chain_length != 0);

            return best_len;
        }

        /* ==========================================================================
        * Fill the window when the lookahead becomes insufficient.
        * Updates strstart and lookahead, and sets eofile if end of input file.
        * IN assertion: lookahead < MIN_LOOKAHEAD && strstart + lookahead > 0
        * OUT assertions: at least one byte has been read, or eofile is set;
        *    file reads are performed for at least two bytes (required for the
        *    translate_eol option).
        */
        var zip_fill_window = function () {
            var n, m;

            // Amount of free space at the end of the window.
            var more = zip_window_size - zip_lookahead - zip_strstart;

            /* If the window is almost full and there is insufficient lookahead,
            * move the upper half to the lower one to make room in the upper half.
            */
            if (more == -1) {
                /* Very unlikely, but possible on 16 bit machine if strstart == 0
                * and lookahead == 1 (input done one byte at time)
                */
                more--;
            } else if (zip_strstart >= zip_WSIZE + zip_MAX_DIST) {
                /* By the IN assertion, the window is not empty so we can't confuse
                * more == 0 with more == 64K on a 16 bit machine.
                */
                //	Assert(window_size == (ulg)2*WSIZE, "no sliding with BIG_MEM");

                //	System.arraycopy(window, WSIZE, window, 0, WSIZE);
                for (n = 0; n < zip_WSIZE; n++)
                    zip_window[n] = zip_window[n + zip_WSIZE];

                zip_match_start -= zip_WSIZE;
                zip_strstart -= zip_WSIZE; /* we now have strstart >= MAX_DIST: */
                zip_block_start -= zip_WSIZE;

                for (n = 0; n < zip_HASH_SIZE; n++) {
                    m = zip_head1(n);
                    zip_head2(n, m >= zip_WSIZE ? m - zip_WSIZE : zip_NIL);
                }
                for (n = 0; n < zip_WSIZE; n++) {
                    /* If n is not on any hash chain, prev[n] is garbage but
                    * its value will never be used.
                    */
                    m = zip_prev[n];
                    zip_prev[n] = (m >= zip_WSIZE ? m - zip_WSIZE : zip_NIL);
                }
                more += zip_WSIZE;
            }
            // At this point, more >= 2
            if (!zip_eofile) {
                n = zip_read_buff(zip_window, zip_strstart + zip_lookahead, more);
                if (n <= 0)
                    zip_eofile = true;
                else
                    zip_lookahead += n;
            }
        }

        /* ==========================================================================
        * Processes a new input file and return its compressed length. This
        * function does not perform lazy evaluationof matches and inserts
        * new strings in the dictionary only for unmatched strings or for short
        * matches. It is used only for the fast compression options.
        */
        var zip_deflate_fast = function () {
            while (zip_lookahead != 0 && zip_qhead == null) {
                var flush; // set if current block must be flushed

                /* Insert the string window[strstart .. strstart+2] in the
                * dictionary, and set hash_head to the head of the hash chain:
                */
                zip_INSERT_STRING();

                /* Find the longest match, discarding those <= prev_length.
                * At this point we have always match_length < MIN_MATCH
                */
                if (zip_hash_head != zip_NIL &&
	   zip_strstart - zip_hash_head <= zip_MAX_DIST) {
                    /* To simplify the code, we prevent matches with the string
                    * of window index 0 (in particular we have to avoid a match
                    * of the string with itself at the start of the input file).
                    */
                    zip_match_length = zip_longest_match(zip_hash_head);
                    /* longest_match() sets match_start */
                    if (zip_match_length > zip_lookahead)
                        zip_match_length = zip_lookahead;
                }
                if (zip_match_length >= zip_MIN_MATCH) {
                    //	    check_match(strstart, match_start, match_length);

                    flush = zip_ct_tally(zip_strstart - zip_match_start,
				 zip_match_length - zip_MIN_MATCH);
                    zip_lookahead -= zip_match_length;

                    /* Insert new strings in the hash table only if the match length
                    * is not too large. This saves time but degrades compression.
                    */
                    if (zip_match_length <= zip_max_lazy_match) {
                        zip_match_length--; // string at strstart already in hash table
                        do {
                            zip_strstart++;
                            zip_INSERT_STRING();
                            /* strstart never exceeds WSIZE-MAX_MATCH, so there are
                            * always MIN_MATCH bytes ahead. If lookahead < MIN_MATCH
                            * these bytes are garbage, but it does not matter since
                            * the next lookahead bytes will be emitted as literals.
                            */
                        } while (--zip_match_length != 0);
                        zip_strstart++;
                    } else {
                        zip_strstart += zip_match_length;
                        zip_match_length = 0;
                        zip_ins_h = zip_window[zip_strstart] & 0xff;
                        //		UPDATE_HASH(ins_h, window[strstart + 1]);
                        zip_ins_h = ((zip_ins_h << zip_H_SHIFT) ^ (zip_window[zip_strstart + 1] & 0xff)) & zip_HASH_MASK;

                        //#if MIN_MATCH != 3
                        //		Call UPDATE_HASH() MIN_MATCH-3 more times
                        //#endif

                    }
                } else {
                    /* No match, output a literal byte */
                    flush = zip_ct_tally(0, zip_window[zip_strstart] & 0xff);
                    zip_lookahead--;
                    zip_strstart++;
                }
                if (flush) {
                    zip_flush_block(0);
                    zip_block_start = zip_strstart;
                }

                /* Make sure that we always have enough lookahead, except
                * at the end of the input file. We need MAX_MATCH bytes
                * for the next match, plus MIN_MATCH bytes to insert the
                * string following the next match.
                */
                while (zip_lookahead < zip_MIN_LOOKAHEAD && !zip_eofile)
                    zip_fill_window();
            }
        }

        var zip_deflate_better = function () {
            /* Process the input block. */
            while (zip_lookahead != 0 && zip_qhead == null) {
                /* Insert the string window[strstart .. strstart+2] in the
                * dictionary, and set hash_head to the head of the hash chain:
                */
                zip_INSERT_STRING();

                /* Find the longest match, discarding those <= prev_length.
                */
                zip_prev_length = zip_match_length;
                zip_prev_match = zip_match_start;
                zip_match_length = zip_MIN_MATCH - 1;

                if (zip_hash_head != zip_NIL &&
	   zip_prev_length < zip_max_lazy_match &&
	   zip_strstart - zip_hash_head <= zip_MAX_DIST) {
                    /* To simplify the code, we prevent matches with the string
                    * of window index 0 (in particular we have to avoid a match
                    * of the string with itself at the start of the input file).
                    */
                    zip_match_length = zip_longest_match(zip_hash_head);
                    /* longest_match() sets match_start */
                    if (zip_match_length > zip_lookahead)
                        zip_match_length = zip_lookahead;

                    /* Ignore a length 3 match if it is too distant: */
                    if (zip_match_length == zip_MIN_MATCH &&
	       zip_strstart - zip_match_start > zip_TOO_FAR) {
                        /* If prev_match is also MIN_MATCH, match_start is garbage
                        * but we will ignore the current match anyway.
                        */
                        zip_match_length--;
                    }
                }
                /* If there was a match at the previous step and the current
                * match is not better, output the previous match:
                */
                if (zip_prev_length >= zip_MIN_MATCH &&
	   zip_match_length <= zip_prev_length) {
                    var flush; // set if current block must be flushed

                    //	    check_match(strstart - 1, prev_match, prev_length);
                    flush = zip_ct_tally(zip_strstart - 1 - zip_prev_match,
				 zip_prev_length - zip_MIN_MATCH);

                    /* Insert in hash table all strings up to the end of the match.
                    * strstart-1 and strstart are already inserted.
                    */
                    zip_lookahead -= zip_prev_length - 1;
                    zip_prev_length -= 2;
                    do {
                        zip_strstart++;
                        zip_INSERT_STRING();
                        /* strstart never exceeds WSIZE-MAX_MATCH, so there are
                        * always MIN_MATCH bytes ahead. If lookahead < MIN_MATCH
                        * these bytes are garbage, but it does not matter since the
                        * next lookahead bytes will always be emitted as literals.
                        */
                    } while (--zip_prev_length != 0);
                    zip_match_available = 0;
                    zip_match_length = zip_MIN_MATCH - 1;
                    zip_strstart++;
                    if (flush) {
                        zip_flush_block(0);
                        zip_block_start = zip_strstart;
                    }
                } else if (zip_match_available != 0) {
                    /* If there was no match at the previous position, output a
                    * single literal. If there was a match but the current match
                    * is longer, truncate the previous match to a single literal.
                    */
                    if (zip_ct_tally(0, zip_window[zip_strstart - 1] & 0xff)) {
                        zip_flush_block(0);
                        zip_block_start = zip_strstart;
                    }
                    zip_strstart++;
                    zip_lookahead--;
                } else {
                    /* There is no previous match to compare with, wait for
                    * the next step to decide.
                    */
                    zip_match_available = 1;
                    zip_strstart++;
                    zip_lookahead--;
                }

                /* Make sure that we always have enough lookahead, except
                * at the end of the input file. We need MAX_MATCH bytes
                * for the next match, plus MIN_MATCH bytes to insert the
                * string following the next match.
                */
                while (zip_lookahead < zip_MIN_LOOKAHEAD && !zip_eofile)
                    zip_fill_window();
            }
        }

        var zip_init_deflate = function () {
            if (zip_eofile)
                return;
            zip_bi_buf = 0;
            zip_bi_valid = 0;
            zip_ct_init();
            zip_lm_init();

            zip_qhead = null;
            zip_outcnt = 0;
            zip_outoff = 0;

            if (zip_compr_level <= 3) {
                zip_prev_length = zip_MIN_MATCH - 1;
                zip_match_length = 0;
            }
            else {
                zip_match_length = zip_MIN_MATCH - 1;
                zip_match_available = 0;
            }

            zip_complete = false;
        }

        /* ==========================================================================
        * Same as above, but achieves better compression. We use a lazy
        * evaluation for matches: a match is finally adopted only if there is
        * no better match at the next window position.
        */
        var zip_deflate_internal = function (buff, off, buff_size) {
            var n;

            if (!zip_initflag) {
                zip_init_deflate();
                zip_initflag = true;
                if (zip_lookahead == 0) { // empty
                    zip_complete = true;
                    return 0;
                }
            }

            if ((n = zip_qcopy(buff, off, buff_size)) == buff_size)
                return buff_size;
            if (zip_complete)
                return n;

            if (zip_compr_level <= 3) // optimized for speed
                zip_deflate_fast();
            else
                zip_deflate_better();
            if (zip_lookahead == 0) {
                if (zip_match_available != 0) {
                    zip_ct_tally(0, zip_window[zip_strstart - 1] & 0xff);
                }
                zip_flush_block(1);
                zip_complete = true;
            }
            return n + zip_qcopy(buff, n + off, buff_size - n);
        }

        var zip_qcopy = function (buff, off, buff_size) {
            var n, i, j;

            n = 0;
            while (zip_qhead != null && n < buff_size) {
                i = buff_size - n;
                if (i > zip_qhead.len)
                    i = zip_qhead.len;
                //      System.arraycopy(qhead.ptr, qhead.off, buff, off + n, i);
                for (j = 0; j < i; j++) {
                    buff[off + n + j] = zip_qhead.ptr[zip_qhead.off + j];
                }

                zip_qhead.off += i;
                zip_qhead.len -= i;
                n += i;
                if (zip_qhead.len == 0) {
                    var p;
                    p = zip_qhead;
                    zip_qhead = zip_qhead.next;
                    zip_reuse_queue(p);
                }
            }
            if (n == buff_size) {
                return n;
            }

            if (zip_outoff < zip_outcnt) {
                i = buff_size - n;
                if (i > zip_outcnt - zip_outoff) {
                    i = zip_outcnt - zip_outoff;
                }
                //      System.arraycopy(outbuf, outoff, buff, off + n, i);
                for (j = 0; j < i; j++) {
                    buff[off + n + j] = zip_outbuf[zip_outoff + j];
                }
                zip_outoff += i;
                n += i;
                if (zip_outcnt == zip_outoff) {
                    zip_outcnt = zip_outoff = 0;
                }
            }
            return n;
        }

        /* ==========================================================================
        * Allocate the match buffer, initialize the various tables and save the
        * location of the internal file attribute (ascii/binary) and method
        * (DEFLATE/STORE).
        */
        var zip_ct_init = function () {
            var n; // iterates over tree elements
            var bits; // bit counter
            var length; // length value
            var code; // code value
            var dist; // distance index

            if (zip_static_dtree[0].dl != 0) return; // ct_init already called

            zip_l_desc.dyn_tree = zip_dyn_ltree;
            zip_l_desc.static_tree = zip_static_ltree;
            zip_l_desc.extra_bits = zip_extra_lbits;
            zip_l_desc.extra_base = zip_LITERALS + 1;
            zip_l_desc.elems = zip_L_CODES;
            zip_l_desc.max_length = zip_MAX_BITS;
            zip_l_desc.max_code = 0;

            zip_d_desc.dyn_tree = zip_dyn_dtree;
            zip_d_desc.static_tree = zip_static_dtree;
            zip_d_desc.extra_bits = zip_extra_dbits;
            zip_d_desc.extra_base = 0;
            zip_d_desc.elems = zip_D_CODES;
            zip_d_desc.max_length = zip_MAX_BITS;
            zip_d_desc.max_code = 0;

            zip_bl_desc.dyn_tree = zip_bl_tree;
            zip_bl_desc.static_tree = null;
            zip_bl_desc.extra_bits = zip_extra_blbits;
            zip_bl_desc.extra_base = 0;
            zip_bl_desc.elems = zip_BL_CODES;
            zip_bl_desc.max_length = zip_MAX_BL_BITS;
            zip_bl_desc.max_code = 0;

            // Initialize the mapping length (0..255) -> length code (0..28)
            length = 0;
            for (code = 0; code < zip_LENGTH_CODES - 1; code++) {
                zip_base_length[code] = length;
                for (n = 0; n < (1 << zip_extra_lbits[code]) ; n++)
                    zip_length_code[length++] = code;
            }
            // Assert (length == 256, "ct_init: length != 256");

            /* Note that the length 255 (match length 258) can be represented
            * in two different ways: code 284 + 5 bits or code 285, so we
            * overwrite length_code[255] to use the best encoding:
            */
            zip_length_code[length - 1] = code;

            /* Initialize the mapping dist (0..32K) -> dist code (0..29) */
            dist = 0;
            for (code = 0; code < 16; code++) {
                zip_base_dist[code] = dist;
                for (n = 0; n < (1 << zip_extra_dbits[code]) ; n++) {
                    zip_dist_code[dist++] = code;
                }
            }
            // Assert (dist == 256, "ct_init: dist != 256");
            dist >>= 7; // from now on, all distances are divided by 128
            for (; code < zip_D_CODES; code++) {
                zip_base_dist[code] = dist << 7;
                for (n = 0; n < (1 << (zip_extra_dbits[code] - 7)) ; n++)
                    zip_dist_code[256 + dist++] = code;
            }
            // Assert (dist == 256, "ct_init: 256+dist != 512");

            // Construct the codes of the static literal tree
            for (bits = 0; bits <= zip_MAX_BITS; bits++)
                zip_bl_count[bits] = 0;
            n = 0;
            while (n <= 143) { zip_static_ltree[n++].dl = 8; zip_bl_count[8]++; }
            while (n <= 255) { zip_static_ltree[n++].dl = 9; zip_bl_count[9]++; }
            while (n <= 279) { zip_static_ltree[n++].dl = 7; zip_bl_count[7]++; }
            while (n <= 287) { zip_static_ltree[n++].dl = 8; zip_bl_count[8]++; }
            /* Codes 286 and 287 do not exist, but we must include them in the
            * tree construction to get a canonical Huffman tree (longest code
            * all ones)
            */
            zip_gen_codes(zip_static_ltree, zip_L_CODES + 1);

            /* The static distance tree is trivial: */
            for (n = 0; n < zip_D_CODES; n++) {
                zip_static_dtree[n].dl = 5;
                zip_static_dtree[n].fc = zip_bi_reverse(n, 5);
            }

            // Initialize the first block of the first file:
            zip_init_block();
        }

        /* ==========================================================================
        * Initialize a new block.
        */
        var zip_init_block = function () {
            var n; // iterates over tree elements

            // Initialize the trees.
            for (n = 0; n < zip_L_CODES; n++) zip_dyn_ltree[n].fc = 0;
            for (n = 0; n < zip_D_CODES; n++) zip_dyn_dtree[n].fc = 0;
            for (n = 0; n < zip_BL_CODES; n++) zip_bl_tree[n].fc = 0;

            zip_dyn_ltree[zip_END_BLOCK].fc = 1;
            zip_opt_len = zip_static_len = 0;
            zip_last_lit = zip_last_dist = zip_last_flags = 0;
            zip_flags = 0;
            zip_flag_bit = 1;
        }

        /* ==========================================================================
        * Restore the heap property by moving down the tree starting at node k,
        * exchanging a node with the smallest of its two sons if necessary, stopping
        * when the heap property is re-established (each father smaller than its
        * two sons).
        */
        var zip_pqdownheap = function (
    tree, // the tree to restore
    k) {	// node to move down
            var v = zip_heap[k];
            var j = k << 1; // left son of k

            while (j <= zip_heap_len) {
                // Set j to the smallest of the two sons:
                if (j < zip_heap_len &&
	   zip_SMALLER(tree, zip_heap[j + 1], zip_heap[j]))
                    j++;

                // Exit if v is smaller than both sons
                if (zip_SMALLER(tree, v, zip_heap[j]))
                    break;

                // Exchange v with the smallest son
                zip_heap[k] = zip_heap[j];
                k = j;

                // And continue down the tree, setting j to the left son of k
                j <<= 1;
            }
            zip_heap[k] = v;
        }

        /* ==========================================================================
        * Compute the optimal bit lengths for a tree and update the total bit length
        * for the current block.
        * IN assertion: the fields freq and dad are set, heap[heap_max] and
        *    above are the tree nodes sorted by increasing frequency.
        * OUT assertions: the field len is set to the optimal bit length, the
        *     array bl_count contains the frequencies for each bit length.
        *     The length opt_len is updated; static_len is also updated if stree is
        *     not null.
        */
        var zip_gen_bitlen = function (desc) { // the tree descriptor
            var tree = desc.dyn_tree;
            var extra = desc.extra_bits;
            var base = desc.extra_base;
            var max_code = desc.max_code;
            var max_length = desc.max_length;
            var stree = desc.static_tree;
            var h; 	// heap index
            var n, m; 	// iterate over the tree elements
            var bits; 	// bit length
            var xbits; 	// extra bits
            var f; 	// frequency
            var overflow = 0; // number of elements with bit length too large

            for (bits = 0; bits <= zip_MAX_BITS; bits++)
                zip_bl_count[bits] = 0;

            /* In a first pass, compute the optimal bit lengths (which may
            * overflow in the case of the bit length tree).
            */
            tree[zip_heap[zip_heap_max]].dl = 0; // root of the heap

            for (h = zip_heap_max + 1; h < zip_HEAP_SIZE; h++) {
                n = zip_heap[h];
                bits = tree[tree[n].dl].dl + 1;
                if (bits > max_length) {
                    bits = max_length;
                    overflow++;
                }
                tree[n].dl = bits;
                // We overwrite tree[n].dl which is no longer needed

                if (n > max_code)
                    continue; // not a leaf node

                zip_bl_count[bits]++;
                xbits = 0;
                if (n >= base)
                    xbits = extra[n - base];
                f = tree[n].fc;
                zip_opt_len += f * (bits + xbits);
                if (stree != null)
                    zip_static_len += f * (stree[n].dl + xbits);
            }
            if (overflow == 0)
                return;

            // This happens for example on obj2 and pic of the Calgary corpus

            // Find the first bit length which could increase:
            do {
                bits = max_length - 1;
                while (zip_bl_count[bits] == 0)
                    bits--;
                zip_bl_count[bits]--; 	// move one leaf down the tree
                zip_bl_count[bits + 1] += 2; // move one overflow item as its brother
                zip_bl_count[max_length]--;
                /* The brother of the overflow item also moves one step up,
                * but this does not affect bl_count[max_length]
                */
                overflow -= 2;
            } while (overflow > 0);

            /* Now recompute all bit lengths, scanning in increasing frequency.
            * h is still equal to HEAP_SIZE. (It is simpler to reconstruct all
            * lengths instead of fixing only the wrong ones. This idea is taken
            * from 'ar' written by Haruhiko Okumura.)
            */
            for (bits = max_length; bits != 0; bits--) {
                n = zip_bl_count[bits];
                while (n != 0) {
                    m = zip_heap[--h];
                    if (m > max_code)
                        continue;
                    if (tree[m].dl != bits) {
                        zip_opt_len += (bits - tree[m].dl) * tree[m].fc;
                        tree[m].fc = bits;
                    }
                    n--;
                }
            }
        }

        /* ==========================================================================
        * Generate the codes for a given tree and bit counts (which need not be
        * optimal).
        * IN assertion: the array bl_count contains the bit length statistics for
        * the given tree and the field len is set for all tree elements.
        * OUT assertion: the field code is set for all tree elements of non
        *     zero code length.
        */
        var zip_gen_codes = function (tree, // the tree to decorate
		   max_code) {	// largest code with non zero frequency
            var next_code = new Array(zip_MAX_BITS + 1); // next code value for each bit length
            var code = 0; 	// running code value
            var bits; 		// bit index
            var n; 		// code index

            /* The distribution counts are first used to generate the code values
            * without bit reversal.
            */
            for (bits = 1; bits <= zip_MAX_BITS; bits++) {
                code = ((code + zip_bl_count[bits - 1]) << 1);
                next_code[bits] = code;
            }

            /* Check that the bit counts in bl_count are consistent. The last code
            * must be all ones.
            */
            //    Assert (code + encoder->bl_count[MAX_BITS]-1 == (1<<MAX_BITS)-1,
            //	    "inconsistent bit counts");
            //    Tracev((stderr,"\ngen_codes: max_code %d ", max_code));

            for (n = 0; n <= max_code; n++) {
                var len = tree[n].dl;
                if (len == 0)
                    continue;
                // Now reverse the bits
                tree[n].fc = zip_bi_reverse(next_code[len]++, len);

                //      Tracec(tree != static_ltree, (stderr,"\nn %3d %c l %2d c %4x (%x) ",
                //	  n, (isgraph(n) ? n : ' '), len, tree[n].fc, next_code[len]-1));
            }
        }

        /* ==========================================================================
        * Construct one Huffman tree and assigns the code bit strings and lengths.
        * Update the total bit length for the current block.
        * IN assertion: the field freq is set for all tree elements.
        * OUT assertions: the fields len and code are set to the optimal bit length
        *     and corresponding code. The length opt_len is updated; static_len is
        *     also updated if stree is not null. The field max_code is set.
        */
        var zip_build_tree = function (desc) { // the tree descriptor
            var tree = desc.dyn_tree;
            var stree = desc.static_tree;
            var elems = desc.elems;
            var n, m; 	// iterate over heap elements
            var max_code = -1; // largest code with non zero frequency
            var node = elems; // next internal node of the tree

            /* Construct the initial heap, with least frequent element in
            * heap[SMALLEST]. The sons of heap[n] are heap[2*n] and heap[2*n+1].
            * heap[0] is not used.
            */
            zip_heap_len = 0;
            zip_heap_max = zip_HEAP_SIZE;

            for (n = 0; n < elems; n++) {
                if (tree[n].fc != 0) {
                    zip_heap[++zip_heap_len] = max_code = n;
                    zip_depth[n] = 0;
                } else
                    tree[n].dl = 0;
            }

            /* The pkzip format requires that at least one distance code exists,
            * and that at least one bit should be sent even if there is only one
            * possible code. So to avoid special checks later on we force at least
            * two codes of non zero frequency.
            */
            while (zip_heap_len < 2) {
                var xnew = zip_heap[++zip_heap_len] = (max_code < 2 ? ++max_code : 0);
                tree[xnew].fc = 1;
                zip_depth[xnew] = 0;
                zip_opt_len--;
                if (stree != null)
                    zip_static_len -= stree[xnew].dl;
                // new is 0 or 1 so it does not have extra bits
            }
            desc.max_code = max_code;

            /* The elements heap[heap_len/2+1 .. heap_len] are leaves of the tree,
            * establish sub-heaps of increasing lengths:
            */
            for (n = zip_heap_len >> 1; n >= 1; n--)
                zip_pqdownheap(tree, n);

            /* Construct the Huffman tree by repeatedly combining the least two
            * frequent nodes.
            */
            do {
                n = zip_heap[zip_SMALLEST];
                zip_heap[zip_SMALLEST] = zip_heap[zip_heap_len--];
                zip_pqdownheap(tree, zip_SMALLEST);

                m = zip_heap[zip_SMALLEST];  // m = node of next least frequency

                // keep the nodes sorted by frequency
                zip_heap[--zip_heap_max] = n;
                zip_heap[--zip_heap_max] = m;

                // Create a new node father of n and m
                tree[node].fc = tree[n].fc + tree[m].fc;
                //	depth[node] = (char)(MAX(depth[n], depth[m]) + 1);
                if (zip_depth[n] > zip_depth[m] + 1)
                    zip_depth[node] = zip_depth[n];
                else
                    zip_depth[node] = zip_depth[m] + 1;
                tree[n].dl = tree[m].dl = node;

                // and insert the new node in the heap
                zip_heap[zip_SMALLEST] = node++;
                zip_pqdownheap(tree, zip_SMALLEST);

            } while (zip_heap_len >= 2);

            zip_heap[--zip_heap_max] = zip_heap[zip_SMALLEST];

            /* At this point, the fields freq and dad are set. We can now
            * generate the bit lengths.
            */
            zip_gen_bitlen(desc);

            // The field len is now set, we can generate the bit codes
            zip_gen_codes(tree, max_code);
        }

        /* ==========================================================================
        * Scan a literal or distance tree to determine the frequencies of the codes
        * in the bit length tree. Updates opt_len to take into account the repeat
        * counts. (The contribution of the bit length codes will be added later
        * during the construction of bl_tree.)
        */
        var zip_scan_tree = function (tree, // the tree to be scanned
		       max_code) {  // and its largest code of non zero frequency
            var n; 		// iterates over all tree elements
            var prevlen = -1; 	// last emitted length
            var curlen; 		// length of current code
            var nextlen = tree[0].dl; // length of next code
            var count = 0; 	// repeat count of the current code
            var max_count = 7; 	// max repeat count
            var min_count = 4; 	// min repeat count

            if (nextlen == 0) {
                max_count = 138;
                min_count = 3;
            }
            tree[max_code + 1].dl = 0xffff; // guard

            for (n = 0; n <= max_code; n++) {
                curlen = nextlen;
                nextlen = tree[n + 1].dl;
                if (++count < max_count && curlen == nextlen)
                    continue;
                else if (count < min_count)
                    zip_bl_tree[curlen].fc += count;
                else if (curlen != 0) {
                    if (curlen != prevlen)
                        zip_bl_tree[curlen].fc++;
                    zip_bl_tree[zip_REP_3_6].fc++;
                } else if (count <= 10)
                    zip_bl_tree[zip_REPZ_3_10].fc++;
                else
                    zip_bl_tree[zip_REPZ_11_138].fc++;
                count = 0; prevlen = curlen;
                if (nextlen == 0) {
                    max_count = 138;
                    min_count = 3;
                } else if (curlen == nextlen) {
                    max_count = 6;
                    min_count = 3;
                } else {
                    max_count = 7;
                    min_count = 4;
                }
            }
        }

        /* ==========================================================================
        * Send a literal or distance tree in compressed form, using the codes in
        * bl_tree.
        */
        var zip_send_tree = function (tree, // the tree to be scanned
		   max_code) { // and its largest code of non zero frequency
            var n; 		// iterates over all tree elements
            var prevlen = -1; 	// last emitted length
            var curlen; 		// length of current code
            var nextlen = tree[0].dl; // length of next code
            var count = 0; 	// repeat count of the current code
            var max_count = 7; 	// max repeat count
            var min_count = 4; 	// min repeat count

            /* tree[max_code+1].dl = -1; */  /* guard already set */
            if (nextlen == 0) {
                max_count = 138;
                min_count = 3;
            }

            for (n = 0; n <= max_code; n++) {
                curlen = nextlen;
                nextlen = tree[n + 1].dl;
                if (++count < max_count && curlen == nextlen) {
                    continue;
                } else if (count < min_count) {
                    do { zip_SEND_CODE(curlen, zip_bl_tree); } while (--count != 0);
                } else if (curlen != 0) {
                    if (curlen != prevlen) {
                        zip_SEND_CODE(curlen, zip_bl_tree);
                        count--;
                    }
                    // Assert(count >= 3 && count <= 6, " 3_6?");
                    zip_SEND_CODE(zip_REP_3_6, zip_bl_tree);
                    zip_send_bits(count - 3, 2);
                } else if (count <= 10) {
                    zip_SEND_CODE(zip_REPZ_3_10, zip_bl_tree);
                    zip_send_bits(count - 3, 3);
                } else {
                    zip_SEND_CODE(zip_REPZ_11_138, zip_bl_tree);
                    zip_send_bits(count - 11, 7);
                }
                count = 0;
                prevlen = curlen;
                if (nextlen == 0) {
                    max_count = 138;
                    min_count = 3;
                } else if (curlen == nextlen) {
                    max_count = 6;
                    min_count = 3;
                } else {
                    max_count = 7;
                    min_count = 4;
                }
            }
        }

        /* ==========================================================================
        * Construct the Huffman tree for the bit lengths and return the index in
        * bl_order of the last bit length code to send.
        */
        var zip_build_bl_tree = function () {
            var max_blindex;  // index of last bit length code of non zero freq

            // Determine the bit length frequencies for literal and distance trees
            zip_scan_tree(zip_dyn_ltree, zip_l_desc.max_code);
            zip_scan_tree(zip_dyn_dtree, zip_d_desc.max_code);

            // Build the bit length tree:
            zip_build_tree(zip_bl_desc);
            /* opt_len now includes the length of the tree representations, except
            * the lengths of the bit lengths codes and the 5+5+4 bits for the counts.
            */

            /* Determine the number of bit length codes to send. The pkzip format
            * requires that at least 4 bit length codes be sent. (appnote.txt says
            * 3 but the actual value used is 4.)
            */
            for (max_blindex = zip_BL_CODES - 1; max_blindex >= 3; max_blindex--) {
                if (zip_bl_tree[zip_bl_order[max_blindex]].dl != 0) break;
            }
            /* Update opt_len to include the bit length tree and counts */
            zip_opt_len += 3 * (max_blindex + 1) + 5 + 5 + 4;
            //    Tracev((stderr, "\ndyn trees: dyn %ld, stat %ld",
            //	    encoder->opt_len, encoder->static_len));

            return max_blindex;
        }

        /* ==========================================================================
        * Send the header for a block using dynamic Huffman trees: the counts, the
        * lengths of the bit length codes, the literal tree and the distance tree.
        * IN assertion: lcodes >= 257, dcodes >= 1, blcodes >= 4.
        */
        var zip_send_all_trees = function (lcodes, dcodes, blcodes) { // number of codes for each tree
            var rank; // index in bl_order

            //    Assert (lcodes >= 257 && dcodes >= 1 && blcodes >= 4, "not enough codes");
            //    Assert (lcodes <= L_CODES && dcodes <= D_CODES && blcodes <= BL_CODES,
            //	    "too many codes");
            //    Tracev((stderr, "\nbl counts: "));
            zip_send_bits(lcodes - 257, 5); // not +255 as stated in appnote.txt
            zip_send_bits(dcodes - 1, 5);
            zip_send_bits(blcodes - 4, 4); // not -3 as stated in appnote.txt
            for (rank = 0; rank < blcodes; rank++) {
                //      Tracev((stderr, "\nbl code %2d ", bl_order[rank]));
                zip_send_bits(zip_bl_tree[zip_bl_order[rank]].dl, 3);
            }

            // send the literal tree
            zip_send_tree(zip_dyn_ltree, lcodes - 1);

            // send the distance tree
            zip_send_tree(zip_dyn_dtree, dcodes - 1);
        }

        /* ==========================================================================
        * Determine the best encoding for the current block: dynamic trees, static
        * trees or store, and output the encoded block to the zip file.
        */
        var zip_flush_block = function (eof) { // true if this is the last block for a file
            var opt_lenb, static_lenb; // opt_len and static_len in bytes
            var max_blindex; // index of last bit length code of non zero freq
            var stored_len; // length of input block

            stored_len = zip_strstart - zip_block_start;
            zip_flag_buf[zip_last_flags] = zip_flags; // Save the flags for the last 8 items

            // Construct the literal and distance trees
            zip_build_tree(zip_l_desc);
            //    Tracev((stderr, "\nlit data: dyn %ld, stat %ld",
            //	    encoder->opt_len, encoder->static_len));

            zip_build_tree(zip_d_desc);
            //    Tracev((stderr, "\ndist data: dyn %ld, stat %ld",
            //	    encoder->opt_len, encoder->static_len));
            /* At this point, opt_len and static_len are the total bit lengths of
            * the compressed block data, excluding the tree representations.
            */

            /* Build the bit length tree for the above two trees, and get the index
            * in bl_order of the last bit length code to send.
            */
            max_blindex = zip_build_bl_tree();

            // Determine the best encoding. Compute first the block length in bytes
            opt_lenb = (zip_opt_len + 3 + 7) >> 3;
            static_lenb = (zip_static_len + 3 + 7) >> 3;

            //    Trace((stderr, "\nopt %lu(%lu) stat %lu(%lu) stored %lu lit %u dist %u ",
            //	   opt_lenb, encoder->opt_len,
            //	   static_lenb, encoder->static_len, stored_len,
            //	   encoder->last_lit, encoder->last_dist));

            if (static_lenb <= opt_lenb)
                opt_lenb = static_lenb;
            if (stored_len + 4 <= opt_lenb // 4: two words for the lengths
       && zip_block_start >= 0) {
                var i;

                /* The test buf != NULL is only necessary if LIT_BUFSIZE > WSIZE.
                * Otherwise we can't have processed more than WSIZE input bytes since
                * the last block flush, because compression would have been
                * successful. If LIT_BUFSIZE <= WSIZE, it is never too late to
                * transform a block into a stored block.
                */
                zip_send_bits((zip_STORED_BLOCK << 1) + eof, 3);  /* send block type */
                zip_bi_windup(); 	 /* align on byte boundary */
                zip_put_short(stored_len);
                zip_put_short(~stored_len);

                // copy block
                /*
                p = &window[block_start];
                for(i = 0; i < stored_len; i++)
                put_byte(p[i]);
                */
                for (i = 0; i < stored_len; i++)
                    zip_put_byte(zip_window[zip_block_start + i]);

            } else if (static_lenb == opt_lenb) {
                zip_send_bits((zip_STATIC_TREES << 1) + eof, 3);
                zip_compress_block(zip_static_ltree, zip_static_dtree);
            } else {
                zip_send_bits((zip_DYN_TREES << 1) + eof, 3);
                zip_send_all_trees(zip_l_desc.max_code + 1,
			   zip_d_desc.max_code + 1,
			   max_blindex + 1);
                zip_compress_block(zip_dyn_ltree, zip_dyn_dtree);
            }

            zip_init_block();

            if (eof != 0)
                zip_bi_windup();
        }

        /* ==========================================================================
        * Save the match info and tally the frequency counts. Return true if
        * the current block must be flushed.
        */
        var zip_ct_tally = function (
	dist, // distance of matched string
	lc) { // match length-MIN_MATCH or unmatched char (if dist==0)
            zip_l_buf[zip_last_lit++] = lc;
            if (dist == 0) {
                // lc is the unmatched char
                zip_dyn_ltree[lc].fc++;
            } else {
                // Here, lc is the match length - MIN_MATCH
                dist--; 	    // dist = match distance - 1
                //      Assert((ush)dist < (ush)MAX_DIST &&
                //	     (ush)lc <= (ush)(MAX_MATCH-MIN_MATCH) &&
                //	     (ush)D_CODE(dist) < (ush)D_CODES,  "ct_tally: bad match");

                zip_dyn_ltree[zip_length_code[lc] + zip_LITERALS + 1].fc++;
                zip_dyn_dtree[zip_D_CODE(dist)].fc++;

                zip_d_buf[zip_last_dist++] = dist;
                zip_flags |= zip_flag_bit;
            }
            zip_flag_bit <<= 1;

            // Output the flags if they fill a byte
            if ((zip_last_lit & 7) == 0) {
                zip_flag_buf[zip_last_flags++] = zip_flags;
                zip_flags = 0;
                zip_flag_bit = 1;
            }
            // Try to guess if it is profitable to stop the current block here
            if (zip_compr_level > 2 && (zip_last_lit & 0xfff) == 0) {
                // Compute an upper bound for the compressed length
                var out_length = zip_last_lit * 8;
                var in_length = zip_strstart - zip_block_start;
                var dcode;

                for (dcode = 0; dcode < zip_D_CODES; dcode++) {
                    out_length += zip_dyn_dtree[dcode].fc * (5 + zip_extra_dbits[dcode]);
                }
                out_length >>= 3;
                //      Trace((stderr,"\nlast_lit %u, last_dist %u, in %ld, out ~%ld(%ld%%) ",
                //	     encoder->last_lit, encoder->last_dist, in_length, out_length,
                //	     100L - out_length*100L/in_length));
                if (zip_last_dist < parseInt(zip_last_lit / 2, 10) &&
	   out_length < parseInt(in_length / 2, 10))
                    return true;
            }
            return (zip_last_lit == zip_LIT_BUFSIZE - 1 ||
	    zip_last_dist == zip_DIST_BUFSIZE);
            /* We avoid equality with LIT_BUFSIZE because of wraparound at 64K
            * on 16 bit machines and because stored blocks are restricted to
            * 64K-1 bytes.
            */
        }

        /* ==========================================================================
        * Send the block data compressed using the given Huffman trees
        */
        var zip_compress_block = function (
	ltree, // literal tree
	dtree) {	// distance tree
            var dist; 	// distance of matched string
            var lc; 	// match length or unmatched char (if dist == 0)
            var lx = 0; 	// running index in l_buf
            var dx = 0; 	// running index in d_buf
            var fx = 0; 	// running index in flag_buf
            var flag = 0; // current flags
            var code; 	// the code to send
            var extra; 	// number of extra bits to send

            if (zip_last_lit != 0) do {
                if ((lx & 7) == 0)
                    flag = zip_flag_buf[fx++];
                lc = zip_l_buf[lx++] & 0xff;
                if ((flag & 1) == 0) {
                    zip_SEND_CODE(lc, ltree); /* send a literal byte */
                    //	Tracecv(isgraph(lc), (stderr," '%c' ", lc));
                } else {
                    // Here, lc is the match length - MIN_MATCH
                    code = zip_length_code[lc];
                    zip_SEND_CODE(code + zip_LITERALS + 1, ltree); // send the length code
                    extra = zip_extra_lbits[code];
                    if (extra != 0) {
                        lc -= zip_base_length[code];
                        zip_send_bits(lc, extra); // send the extra length bits
                    }
                    dist = zip_d_buf[dx++];
                    // Here, dist is the match distance - 1
                    code = zip_D_CODE(dist);
                    //	Assert (code < D_CODES, "bad d_code");

                    zip_SEND_CODE(code, dtree);   // send the distance code
                    extra = zip_extra_dbits[code];
                    if (extra != 0) {
                        dist -= zip_base_dist[code];
                        zip_send_bits(dist, extra);   // send the extra distance bits
                    }
                } // literal or match pair ?
                flag >>= 1;
            } while (lx < zip_last_lit);

            zip_SEND_CODE(zip_END_BLOCK, ltree);
        }

        /* ==========================================================================
        * Send a value on a given number of bits.
        * IN assertion: length <= 16 and value fits in length bits.
        */
        var zip_Buf_size = 16; // bit size of bi_buf
        var zip_send_bits = function (
	value, // value to send
	length) {	// number of bits
            /* If not enough room in bi_buf, use (valid) bits from bi_buf and
            * (16 - bi_valid) bits from value, leaving (width - (16-bi_valid))
            * unused bits in value.
            */
            if (zip_bi_valid > zip_Buf_size - length) {
                zip_bi_buf |= (value << zip_bi_valid);
                zip_put_short(zip_bi_buf);
                zip_bi_buf = (value >> (zip_Buf_size - zip_bi_valid));
                zip_bi_valid += length - zip_Buf_size;
            } else {
                zip_bi_buf |= value << zip_bi_valid;
                zip_bi_valid += length;
            }
        }

        /* ==========================================================================
        * Reverse the first len bits of a code, using straightforward code (a faster
        * method would use a table)
        * IN assertion: 1 <= len <= 15
        */
        var zip_bi_reverse = function (
	code, // the value to invert
	len) {	// its bit length
            var res = 0;
            do {
                res |= code & 1;
                code >>= 1;
                res <<= 1;
            } while (--len > 0);
            return res >> 1;
        }

        /* ==========================================================================
        * Write out any remaining bits in an incomplete byte.
        */
        var zip_bi_windup = function () {
            if (zip_bi_valid > 8) {
                zip_put_short(zip_bi_buf);
            } else if (zip_bi_valid > 0) {
                zip_put_byte(zip_bi_buf);
            }
            zip_bi_buf = 0;
            zip_bi_valid = 0;
        }

        var zip_qoutbuf = function () {
            if (zip_outcnt != 0) {
                var q, i;
                q = zip_new_queue();
                if (zip_qhead == null)
                    zip_qhead = zip_qtail = q;
                else
                    zip_qtail = zip_qtail.next = q;
                q.len = zip_outcnt - zip_outoff;
                //      System.arraycopy(zip_outbuf, zip_outoff, q.ptr, 0, q.len);
                for (i = 0; i < q.len; i++)
                    q.ptr[i] = zip_outbuf[zip_outoff + i];
                zip_outcnt = zip_outoff = 0;
            }
        }

        var zip_deflate = function (data, level) {
            var i, j;

            zip_deflate_data = data;
            zip_deflate_pos = 0;
            if (typeof level == "undefined") {
                level = zip_DEFAULT_LEVEL;
            }
            zip_deflate_start(level);
            var buff = [0];
            var aout = [];
            while ((i = zip_deflate_internal(buff, 0, buff.length)) > 0) {
                aout.push(buff[0]);
            }
            zip_deflate_data = null; // G.C.
            return new Uint8Array(aout);
        };

        return zip_deflate;
    })();






    /*
    * $Id: rawinflate.js,v 0.2 2009/03/01 18:32:24 dankogai Exp $
    *
    * original:
    * http://www.onicos.com/staff/iz/amuse/javascript/expert/inflate.txt
    */

    var Inflate = (function () {

        /* Copyright (C) 1999 Masanao Izumo <iz@onicos.co.jp>
        * Version: 1.0.0.1
        * LastModified: Dec 25 1999
        */

        /* Interface:
        * data = zip_inflate(src);
        */

        /* constant parameters */
        var zip_WSIZE = 32768;		// Sliding Window size
        var zip_STORED_BLOCK = 0;
        var zip_STATIC_TREES = 1;
        var zip_DYN_TREES = 2;

        /* for inflate */
        var zip_lbits = 9; 		// bits in base literal/length lookup table
        var zip_dbits = 6; 		// bits in base distance lookup table
        var zip_INBUFSIZ = 32768;	// Input buffer size
        var zip_INBUF_EXTRA = 64;	// Extra buffer

        /* variables (inflate) */
        var zip_slide;
        var zip_wp;			// current position in slide
        var zip_fixed_tl = null;	// inflate static
        var zip_fixed_td;		// inflate static
        var zip_fixed_bl, zip_fixed_bd;	// inflate static
        var zip_bit_buf;		// bit buffer
        var zip_bit_len;		// bits in bit buffer
        var zip_method;
        var zip_eof;
        var zip_copy_leng;
        var zip_copy_dist;
        var zip_tl, zip_td;	// literal/length and distance decoder tables
        var zip_bl, zip_bd;	// number of bits decoded by tl and td

        var zip_inflate_data;
        var zip_inflate_pos;


        /* constant tables (inflate) */
        var zip_MASK_BITS = new Array(
    0x0000,
    0x0001, 0x0003, 0x0007, 0x000f, 0x001f, 0x003f, 0x007f, 0x00ff,
    0x01ff, 0x03ff, 0x07ff, 0x0fff, 0x1fff, 0x3fff, 0x7fff, 0xffff);
        // Tables for deflate from PKZIP's appnote.txt.
        var zip_cplens = new Array( // Copy lengths for literal codes 257..285
            3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 15, 17, 19, 23, 27, 31,
            35, 43, 51, 59, 67, 83, 99, 115, 131, 163, 195, 227, 258, 0, 0);
        /* note: see note #13 above about the 258 in this list. */
        var zip_cplext = new Array( // Extra bits for literal codes 257..285
            0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2,
            3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 0, 99, 99); // 99==invalid
        var zip_cpdist = new Array( // Copy offsets for distance codes 0..29
            1, 2, 3, 4, 5, 7, 9, 13, 17, 25, 33, 49, 65, 97, 129, 193,
            257, 385, 513, 769, 1025, 1537, 2049, 3073, 4097, 6145,
            8193, 12289, 16385, 24577);
        var zip_cpdext = new Array( // Extra bits for distance codes
            0, 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6,
            7, 7, 8, 8, 9, 9, 10, 10, 11, 11,
            12, 12, 13, 13);
        var zip_border = new Array(  // Order of the bit length code lengths
            16, 17, 18, 0, 8, 7, 9, 6, 10, 5, 11, 4, 12, 3, 13, 2, 14, 1, 15);
        /* objects (inflate) */

        var zip_HuftList = function () {
            this.next = null;
            this.list = null;
        }

        var zip_HuftNode = function () {
            this.e = 0; // number of extra bits or operation
            this.b = 0; // number of bits in this code or subcode

            // union
            this.n = 0; // literal, length base, or distance base
            this.t = null; // (zip_HuftNode) pointer to next level of table
        }

        var zip_HuftBuild = function (b,	// code lengths in bits (all assumed <= BMAX)
                n,	// number of codes (assumed <= N_MAX)
                s,	// number of simple-valued codes (0..s-1)
                d,	// list of base values for non-simple codes
                e,	// list of extra bits for non-simple codes
                mm	// maximum lookup bits
            ) {
            this.BMAX = 16;   // maximum bit length of any code
            this.N_MAX = 288; // maximum number of codes in any set
            this.status = 0;	// 0: success, 1: incomplete table, 2: bad input
            this.root = null;	// (zip_HuftList) starting table
            this.m = 0;		// maximum lookup bits, returns actual

            /* Given a list of code lengths and a maximum table size, make a set of
               tables to decode that set of codes.	Return zero on success, one if
               the given code set is incomplete (the tables are still built in this
               case), two if the input is invalid (all zero length codes or an
               oversubscribed set of lengths), and three if not enough memory.
               The code with value 256 is special, and the tables are constructed
               so that no bits beyond that code are fetched when that code is
               decoded. */
            {
                var a;			// counter for codes of length k
                var c = new Array(this.BMAX + 1);	// bit length count table
                var el;			// length of EOB code (value 256)
                var f;			// i repeats in table every f entries
                var g;			// maximum code length
                var h;			// table level
                var i;			// counter, current code
                var j;			// counter
                var k;			// number of bits in current code
                var lx = new Array(this.BMAX + 1);	// stack of bits per table
                var p;			// pointer into c[], b[], or v[]
                var pidx;		// index of p
                var q;			// (zip_HuftNode) points to current table
                var r = new zip_HuftNode(); // table entry for structure assignment
                var u = new Array(this.BMAX); // zip_HuftNode[BMAX][]  table stack
                var v = new Array(this.N_MAX); // values in order of bit length
                var w;
                var x = new Array(this.BMAX + 1);// bit offsets, then code stack
                var xp;			// pointer into x or c
                var y;			// number of dummy codes added
                var z;			// number of entries in current table
                var o;
                var tail;		// (zip_HuftList)

                tail = this.root = null;
                for (i = 0; i < c.length; i++)
                    c[i] = 0;
                for (i = 0; i < lx.length; i++)
                    lx[i] = 0;
                for (i = 0; i < u.length; i++)
                    u[i] = null;
                for (i = 0; i < v.length; i++)
                    v[i] = 0;
                for (i = 0; i < x.length; i++)
                    x[i] = 0;

                // Generate counts for each bit length
                el = n > 256 ? b[256] : this.BMAX; // set length of EOB code, if any
                p = b; pidx = 0;
                i = n;
                do {
                    c[p[pidx]]++;	// assume all entries <= BMAX
                    pidx++;
                } while (--i > 0);
                if (c[0] == n) {	// null input--all zero length codes
                    this.root = null;
                    this.m = 0;
                    this.status = 0;
                    return;
                }

                // Find minimum and maximum length, bound *m by those
                for (j = 1; j <= this.BMAX; j++)
                    if (c[j] != 0)
                        break;
                k = j;			// minimum code length
                if (mm < j)
                    mm = j;
                for (i = this.BMAX; i != 0; i--)
                    if (c[i] != 0)
                        break;
                g = i;			// maximum code length
                if (mm > i)
                    mm = i;

                // Adjust last length count to fill out codes, if needed
                for (y = 1 << j; j < i; j++, y <<= 1)
                    if ((y -= c[j]) < 0) {
                        this.status = 2;	// bad input: more codes than bits
                        this.m = mm;
                        return;
                    }
                if ((y -= c[i]) < 0) {
                    this.status = 2;
                    this.m = mm;
                    return;
                }
                c[i] += y;

                // Generate starting offsets into the value table for each length
                x[1] = j = 0;
                p = c;
                pidx = 1;
                xp = 2;
                while (--i > 0)		// note that i == g from above
                    x[xp++] = (j += p[pidx++]);

                // Make a table of values in order of bit lengths
                p = b; pidx = 0;
                i = 0;
                do {
                    if ((j = p[pidx++]) != 0)
                        v[x[j]++] = i;
                } while (++i < n);
                n = x[g];			// set n to length of v

                // Generate the Huffman codes and for each, make the table entries
                x[0] = i = 0;		// first Huffman code is zero
                p = v; pidx = 0;		// grab values in bit order
                h = -1;			// no tables yet--level -1
                w = lx[0] = 0;		// no bits decoded yet
                q = null;			// ditto
                z = 0;			// ditto

                // go through the bit lengths (k already is bits in shortest code)
                for (; k <= g; k++) {
                    a = c[k];
                    while (a-- > 0) {
                        // here i is the Huffman code of length k bits for value p[pidx]
                        // make tables up to required level
                        while (k > w + lx[1 + h]) {
                            w += lx[1 + h]; // add bits already decoded
                            h++;

                            // compute minimum size table less than or equal to *m bits
                            z = (z = g - w) > mm ? mm : z; // upper limit
                            if ((f = 1 << (j = k - w)) > a + 1) { // try a k-w bit table
                                // too few codes for k-w bit table
                                f -= a + 1;	// deduct codes from patterns left
                                xp = k;
                                while (++j < z) { // try smaller tables up to z bits
                                    if ((f <<= 1) <= c[++xp])
                                        break;	// enough codes to use up j bits
                                    f -= c[xp];	// else deduct codes from patterns
                                }
                            }
                            if (w + j > el && w < el)
                                j = el - w;	// make EOB code end at table
                            z = 1 << j;	// table entries for j-bit table
                            lx[1 + h] = j; // set table size in stack

                            // allocate and link in new table
                            q = new Array(z);
                            for (o = 0; o < z; o++) {
                                q[o] = new zip_HuftNode();
                            }

                            if (tail == null)
                                tail = this.root = new zip_HuftList();
                            else
                                tail = tail.next = new zip_HuftList();
                            tail.next = null;
                            tail.list = q;
                            u[h] = q;	// table starts after link

                            /* connect to last table, if there is one */
                            if (h > 0) {
                                x[h] = i;		// save pattern for backing up
                                r.b = lx[h];	// bits to dump before this table
                                r.e = 16 + j;	// bits in this table
                                r.t = q;		// pointer to this table
                                j = (i & ((1 << w) - 1)) >> (w - lx[h]);
                                u[h - 1][j].e = r.e;
                                u[h - 1][j].b = r.b;
                                u[h - 1][j].n = r.n;
                                u[h - 1][j].t = r.t;
                            }
                        }

                        // set up table entry in r
                        r.b = k - w;
                        if (pidx >= n)
                            r.e = 99;		// out of values--invalid code
                        else if (p[pidx] < s) {
                            r.e = (p[pidx] < 256 ? 16 : 15); // 256 is end-of-block code
                            r.n = p[pidx++];	// simple code is just the value
                        } else {
                            r.e = e[p[pidx] - s];	// non-simple--look up in lists
                            r.n = d[p[pidx++] - s];
                        }

                        // fill code-like entries with r //
                        f = 1 << (k - w);
                        for (j = i >> w; j < z; j += f) {
                            q[j].e = r.e;
                            q[j].b = r.b;
                            q[j].n = r.n;
                            q[j].t = r.t;
                        }

                        // backwards increment the k-bit code i
                        for (j = 1 << (k - 1) ; (i & j) != 0; j >>= 1)
                            i ^= j;
                        i ^= j;

                        // backup over finished tables
                        while ((i & ((1 << w) - 1)) != x[h]) {
                            w -= lx[h];		// don't need to update q
                            h--;
                        }
                    }
                }

                /* return actual size of base table */
                this.m = lx[1];

                /* Return true (1) if we were given an incomplete table */
                this.status = ((y != 0 && g != 1) ? 1 : 0);
            } /* end of constructor */
        }


        /* routines (inflate) */

        var zip_GET_BYTE = function () {
            if (zip_inflate_data.length == zip_inflate_pos)
                return -1;
            //return zip_inflate_data.charCodeAt(zip_inflate_pos++) & 0xff;
            return zip_inflate_data[zip_inflate_pos++];
        }

        var zip_NEEDBITS = function (n) {
            while (zip_bit_len < n) {
                zip_bit_buf |= zip_GET_BYTE() << zip_bit_len;
                zip_bit_len += 8;
            }
        }

        var zip_GETBITS = function (n) {
            return zip_bit_buf & zip_MASK_BITS[n];
        }

        var zip_DUMPBITS = function (n) {
            zip_bit_buf >>= n;
            zip_bit_len -= n;
        }

        var zip_inflate_codes = function (buff, off, size) {
            /* inflate (decompress) the codes in a deflated (compressed) block.
               Return an error code or zero if it all goes ok. */
            var e;		// table entry flag/number of extra bits
            var t;		// (zip_HuftNode) pointer to table entry
            var n;

            if (size == 0)
                return 0;

            // inflate the coded data
            n = 0;
            for (; ;) {			// do until end of block
                zip_NEEDBITS(zip_bl);
                t = zip_tl.list[zip_GETBITS(zip_bl)];
                e = t.e;
                while (e > 16) {
                    if (e == 99)
                        return -1;
                    zip_DUMPBITS(t.b);
                    e -= 16;
                    zip_NEEDBITS(e);
                    t = t.t[zip_GETBITS(e)];
                    e = t.e;
                }
                zip_DUMPBITS(t.b);

                if (e == 16) {		// then it's a literal
                    zip_wp &= zip_WSIZE - 1;
                    buff[off + n++] = zip_slide[zip_wp++] = t.n;
                    if (n == size)
                        return size;
                    continue;
                }

                // exit if end of block
                if (e == 15)
                    break;

                // it's an EOB or a length

                // get length of block to copy
                zip_NEEDBITS(e);
                zip_copy_leng = t.n + zip_GETBITS(e);
                zip_DUMPBITS(e);

                // decode distance of block to copy
                zip_NEEDBITS(zip_bd);
                t = zip_td.list[zip_GETBITS(zip_bd)];
                e = t.e;

                while (e > 16) {
                    if (e == 99)
                        return -1;
                    zip_DUMPBITS(t.b);
                    e -= 16;
                    zip_NEEDBITS(e);
                    t = t.t[zip_GETBITS(e)];
                    e = t.e;
                }
                zip_DUMPBITS(t.b);
                zip_NEEDBITS(e);
                zip_copy_dist = zip_wp - t.n - zip_GETBITS(e);
                zip_DUMPBITS(e);

                // do the copy
                while (zip_copy_leng > 0 && n < size) {
                    zip_copy_leng--;
                    zip_copy_dist &= zip_WSIZE - 1;
                    zip_wp &= zip_WSIZE - 1;
                    buff[off + n++] = zip_slide[zip_wp++]
                    = zip_slide[zip_copy_dist++];
                }

                if (n == size)
                    return size;
            }

            zip_method = -1; // done
            return n;
        }

        var zip_inflate_stored = function (buff, off, size) {
            /* "decompress" an inflated type 0 (stored) block. */
            var n;

            // go to byte boundary
            n = zip_bit_len & 7;
            zip_DUMPBITS(n);

            // get the length and its complement
            zip_NEEDBITS(16);
            n = zip_GETBITS(16);
            zip_DUMPBITS(16);
            zip_NEEDBITS(16);
            if (n != ((~zip_bit_buf) & 0xffff))
                return -1;			// error in compressed data
            zip_DUMPBITS(16);

            // read and output the compressed data
            zip_copy_leng = n;

            n = 0;
            while (zip_copy_leng > 0 && n < size) {
                zip_copy_leng--;
                zip_wp &= zip_WSIZE - 1;
                zip_NEEDBITS(8);
                buff[off + n++] = zip_slide[zip_wp++] =
                    zip_GETBITS(8);
                zip_DUMPBITS(8);
            }

            if (zip_copy_leng == 0)
                zip_method = -1; // done
            return n;
        }

        var zip_inflate_fixed = function (buff, off, size) {
            /* decompress an inflated type 1 (fixed Huffman codes) block.  We should
               either replace this with a custom decoder, or at least precompute the
               Huffman tables. */

            // if first time, set up tables for fixed blocks
            if (zip_fixed_tl == null) {
                var i;			// temporary variable
                var l = new Array(288);	// length list for huft_build
                var h;	// zip_HuftBuild

                // literal table
                for (i = 0; i < 144; i++)
                    l[i] = 8;
                for (; i < 256; i++)
                    l[i] = 9;
                for (; i < 280; i++)
                    l[i] = 7;
                for (; i < 288; i++)	// make a complete, but wrong code set
                    l[i] = 8;
                zip_fixed_bl = 7;

                h = new zip_HuftBuild(l, 288, 257, zip_cplens, zip_cplext,
                              zip_fixed_bl);
                if (h.status != 0) {
                    alert("HufBuild error: " + h.status);
                    return -1;
                }
                zip_fixed_tl = h.root;
                zip_fixed_bl = h.m;

                // distance table
                for (i = 0; i < 30; i++)	// make an incomplete code set
                    l[i] = 5;
                zip_fixed_bd = 5;

                h = new zip_HuftBuild(l, 30, 0, zip_cpdist, zip_cpdext, zip_fixed_bd);
                if (h.status > 1) {
                    zip_fixed_tl = null;
                    alert("HufBuild error: " + h.status);
                    return -1;
                }
                zip_fixed_td = h.root;
                zip_fixed_bd = h.m;
            }

            zip_tl = zip_fixed_tl;
            zip_td = zip_fixed_td;
            zip_bl = zip_fixed_bl;
            zip_bd = zip_fixed_bd;
            return zip_inflate_codes(buff, off, size);
        }

        var zip_inflate_dynamic = function (buff, off, size) {
            // decompress an inflated type 2 (dynamic Huffman codes) block.
            var i;		// temporary variables
            var j;
            var l;		// last length
            var n;		// number of lengths to get
            var t;		// (zip_HuftNode) literal/length code table
            var nb;		// number of bit length codes
            var nl;		// number of literal/length codes
            var nd;		// number of distance codes
            var ll = new Array(286 + 30); // literal/length and distance code lengths
            var h;		// (zip_HuftBuild)

            for (i = 0; i < ll.length; i++)
                ll[i] = 0;

            // read in table lengths
            zip_NEEDBITS(5);
            nl = 257 + zip_GETBITS(5);	// number of literal/length codes
            zip_DUMPBITS(5);
            zip_NEEDBITS(5);
            nd = 1 + zip_GETBITS(5);	// number of distance codes
            zip_DUMPBITS(5);
            zip_NEEDBITS(4);
            nb = 4 + zip_GETBITS(4);	// number of bit length codes
            zip_DUMPBITS(4);
            if (nl > 286 || nd > 30)
                return -1;		// bad lengths

            // read in bit-length-code lengths
            for (j = 0; j < nb; j++) {
                zip_NEEDBITS(3);
                ll[zip_border[j]] = zip_GETBITS(3);
                zip_DUMPBITS(3);
            }
            for (; j < 19; j++)
                ll[zip_border[j]] = 0;

            // build decoding table for trees--single level, 7 bit lookup
            zip_bl = 7;
            h = new zip_HuftBuild(ll, 19, 19, null, null, zip_bl);
            if (h.status != 0)
                return -1;	// incomplete code set

            zip_tl = h.root;
            zip_bl = h.m;

            // read in literal and distance code lengths
            n = nl + nd;
            i = l = 0;
            while (i < n) {
                zip_NEEDBITS(zip_bl);
                t = zip_tl.list[zip_GETBITS(zip_bl)];
                j = t.b;
                zip_DUMPBITS(j);
                j = t.n;
                if (j < 16)		// length of code in bits (0..15)
                    ll[i++] = l = j;	// save last length in l
                else if (j == 16) {	// repeat last length 3 to 6 times
                    zip_NEEDBITS(2);
                    j = 3 + zip_GETBITS(2);
                    zip_DUMPBITS(2);
                    if (i + j > n)
                        return -1;
                    while (j-- > 0)
                        ll[i++] = l;
                } else if (j == 17) {	// 3 to 10 zero length codes
                    zip_NEEDBITS(3);
                    j = 3 + zip_GETBITS(3);
                    zip_DUMPBITS(3);
                    if (i + j > n)
                        return -1;
                    while (j-- > 0)
                        ll[i++] = 0;
                    l = 0;
                } else {		// j == 18: 11 to 138 zero length codes
                    zip_NEEDBITS(7);
                    j = 11 + zip_GETBITS(7);
                    zip_DUMPBITS(7);
                    if (i + j > n)
                        return -1;
                    while (j-- > 0)
                        ll[i++] = 0;
                    l = 0;
                }
            }

            // build the decoding tables for literal/length and distance codes
            zip_bl = zip_lbits;
            h = new zip_HuftBuild(ll, nl, 257, zip_cplens, zip_cplext, zip_bl);
            if (zip_bl == 0)	// no literals or lengths
                h.status = 1;
            if (h.status != 0) {
                if (h.status == 1)
                    ;// **incomplete literal tree**
                return -1;		// incomplete code set
            }
            zip_tl = h.root;
            zip_bl = h.m;

            for (i = 0; i < nd; i++)
                ll[i] = ll[i + nl];
            zip_bd = zip_dbits;
            h = new zip_HuftBuild(ll, nd, 0, zip_cpdist, zip_cpdext, zip_bd);
            zip_td = h.root;
            zip_bd = h.m;

            if (zip_bd == 0 && nl > 257) {   // lengths but no distances
                // **incomplete distance tree**
                return -1;
            }

            if (h.status == 1) {
                ;// **incomplete distance tree**
            }
            if (h.status != 0)
                return -1;

            // decompress until an end-of-block code
            return zip_inflate_codes(buff, off, size);
        }

        var zip_inflate_start = function () {
            var i;

            if (zip_slide == null)
                zip_slide = new Array(2 * zip_WSIZE);
            zip_wp = 0;
            zip_bit_buf = 0;
            zip_bit_len = 0;
            zip_method = -1;
            zip_eof = false;
            zip_copy_leng = zip_copy_dist = 0;
            zip_tl = null;
        }


        var zip_inflate_internal = function (buff, off, size) {
            // decompress an inflated entry
            var n, i;

            n = 0;
            while (n < size) {
                if (zip_eof && zip_method == -1)
                    return n;

                if (zip_copy_leng > 0) {
                    if (zip_method != zip_STORED_BLOCK) {
                        // STATIC_TREES or DYN_TREES
                        while (zip_copy_leng > 0 && n < size) {
                            zip_copy_leng--;
                            zip_copy_dist &= zip_WSIZE - 1;
                            zip_wp &= zip_WSIZE - 1;
                            buff[off + n++] = zip_slide[zip_wp++] =
                            zip_slide[zip_copy_dist++];
                        }
                    } else {
                        while (zip_copy_leng > 0 && n < size) {
                            zip_copy_leng--;
                            zip_wp &= zip_WSIZE - 1;
                            zip_NEEDBITS(8);
                            buff[off + n++] = zip_slide[zip_wp++] = zip_GETBITS(8);
                            zip_DUMPBITS(8);
                        }
                        if (zip_copy_leng == 0)
                            zip_method = -1; // done
                    }
                    if (n == size)
                        return n;
                }

                if (zip_method == -1) {
                    if (zip_eof)
                        break;

                    // read in last block bit
                    zip_NEEDBITS(1);
                    if (zip_GETBITS(1) != 0)
                        zip_eof = true;
                    zip_DUMPBITS(1);

                    // read in block type
                    zip_NEEDBITS(2);
                    zip_method = zip_GETBITS(2);
                    zip_DUMPBITS(2);
                    zip_tl = null;
                    zip_copy_leng = 0;
                }

                switch (zip_method) {
                    case 0: // zip_STORED_BLOCK
                        i = zip_inflate_stored(buff, off + n, size - n);
                        break;

                    case 1: // zip_STATIC_TREES
                        if (zip_tl != null)
                            i = zip_inflate_codes(buff, off + n, size - n);
                        else
                            i = zip_inflate_fixed(buff, off + n, size - n);
                        break;

                    case 2: // zip_DYN_TREES
                        if (zip_tl != null)
                            i = zip_inflate_codes(buff, off + n, size - n);
                        else
                            i = zip_inflate_dynamic(buff, off + n, size - n);
                        break;

                    default: // error
                        i = -1;
                        break;
                }

                if (i == -1) {
                    if (zip_eof)
                        return 0;
                    return -1;
                }
                n += i;
            }
            return n;
        }


        var zip_inflate = function (data) {
            var i, j, k = 0;

            zip_inflate_start();
            zip_inflate_data = data;
            zip_inflate_pos = 0;
            var buff = new Array(1024);
            var aout = [];
            while ((i = zip_inflate_internal(buff, 0, buff.length)) > 0) {
                //var cbuf = new Array(i);
                for (j = 0; j < i; j++) {
                    //cbuf[j] = String.fromCharCode(buff[j]);
                    //cbuf[j] = (buff[j]);
                    aout[k] = buff[j];
                    k++;
                }
                //aout.push.apply(aout, cbuf);
            }
            zip_inflate_data = null; // G.C.
            //return aout.join("");
            return new Uint8Array(aout);
        }

        return zip_inflate;

    })();

    var adler32 = function (data) {
        var a = 1, b = 0;
        var i;
        var len = data.length;
        var MOD_ADLER = 65521;
        for (i = 0; i < len; i += 1) {
            a = (a + data[i]) % MOD_ADLER;
            b = (b + a) % MOD_ADLER;
        }
        return (b << 16) | a;
    };

    var Compress = function (data, level) {
        data = new Uint8Array(data);
        var calculated_checksum = adler32(data);
        var deflated_data = Deflate(data, level);
        data = new Uint8Array(deflated_data.length + 6);
        data[0] = 0x78;
        data[1] = 0xDA;
        data.set(deflated_data, 2);
        data.set(new Uint8Array(new Int32Array([calculated_checksum]).buffer), data.byteLength - 4);
        return data;
    };

    var array_reverse = function (
	array) {
        var left = null;
        var right = null;
        var length = array.length;
        for (left = 0, right = length - 1; left < right; left += 1, right -= 1) {
            var temporary = array[left];
            array[left] = array[right];
            array[right] = temporary;
        }
        return array;
    }

    var Uncompress = function (data) {
        if (data.byteLength < 6) {
            throw "DataError: Not enough input";
        }
        data = new Uint8Array(data);
        //var checksum = new Int32Array(new Uint8Array(data.subarray(data.byteLength - 4, data.byteLength)).buffer)[0];
        //reverse the checksum bytes and process
        var checksum = new Int32Array(new Uint8Array(array_reverse(data.subarray(data.byteLength - 4, data.byteLength))).buffer)[0];

        var inflated_data = Inflate(data.subarray(2, data.byteLength - 4));
        var calculated_checksum = adler32(inflated_data);
        if (data.byteLength > 6 && inflated_data.byteLength === 0) {
            throw "DataError: Unable to inflate the data";
        }
        else if (checksum !== calculated_checksum) {
            if (checksum > 0)
                throw "DataError: Checksum don't match";
        }
        return inflated_data;
    };

    return {
        'deflate': Deflate,
        'inflate': Inflate,
        'compress': Compress,
        'uncompress': Uncompress
    };

})();

// ... rest of your zlib.js code ...
export default Zlib;