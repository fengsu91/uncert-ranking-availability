/* contrib/spi/insert_username--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION range_window" to load this file. \quit

CREATE FUNCTION range_window(text, text, text, text, integer) RETURNS setof record
AS 'MODULE_PATHNAME', 'range_window'
LANGUAGE C STRICT PARALLEL RESTRICTED;
CREATE FUNCTION range_topk(text, text, integer) RETURNS setof record
AS 'MODULE_PATHNAME', 'range_topk'
LANGUAGE C STRICT PARALLEL RESTRICTED;
