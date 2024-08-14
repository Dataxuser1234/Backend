import "core-js/modules/es.array.push.js";
import _Object$keys from "@babel/runtime-corejs3/core-js-stable/object/keys";
import _Object$getOwnPropertySymbols from "@babel/runtime-corejs3/core-js-stable/object/get-own-property-symbols";
import _filterInstanceProperty from "@babel/runtime-corejs3/core-js-stable/instance/filter";
import _Object$getOwnPropertyDescriptor from "@babel/runtime-corejs3/core-js-stable/object/get-own-property-descriptor";
import _forEachInstanceProperty from "@babel/runtime-corejs3/core-js-stable/instance/for-each";
import _Object$getOwnPropertyDescriptors from "@babel/runtime-corejs3/core-js-stable/object/get-own-property-descriptors";
import _Object$defineProperties from "@babel/runtime-corejs3/core-js-stable/object/define-properties";
import _Object$defineProperty from "@babel/runtime-corejs3/core-js-stable/object/define-property";
import _defineProperty from "@babel/runtime-corejs3/helpers/defineProperty";
function ownKeys(e, r) { var t = _Object$keys(e); if (_Object$getOwnPropertySymbols) { var o = _Object$getOwnPropertySymbols(e); r && (o = _filterInstanceProperty(o).call(o, function (r) { return _Object$getOwnPropertyDescriptor(e, r).enumerable; })), t.push.apply(t, o); } return t; }
function _objectSpread(e) { for (var r = 1; r < arguments.length; r++) { var _context, _context2; var t = null != arguments[r] ? arguments[r] : {}; r % 2 ? _forEachInstanceProperty(_context = ownKeys(Object(t), !0)).call(_context, function (r) { _defineProperty(e, r, t[r]); }) : _Object$getOwnPropertyDescriptors ? _Object$defineProperties(e, _Object$getOwnPropertyDescriptors(t)) : _forEachInstanceProperty(_context2 = ownKeys(Object(t))).call(_context2, function (r) { _Object$defineProperty(e, r, _Object$getOwnPropertyDescriptor(t, r)); }); } return e; }
/**
 *
 */

import { Parser as csvParse } from 'csv-parse';
import { parse as csvParseSync } from 'csv-parse/sync';
import { stringify as csvStringify } from 'csv-stringify';
import { stringify as csvStringifySync } from 'csv-stringify/sync';

/**
 * @private
 */
export function parseCSV(str, options) {
  return csvParseSync(str, _objectSpread(_objectSpread({}, options), {}, {
    columns: true
  }));
}

/**
 * @private
 */
export function toCSV(records, options) {
  return csvStringifySync(records, _objectSpread(_objectSpread({}, options), {}, {
    header: true
  }));
}

/**
 * @private
 */
export function parseCSVStream(options) {
  return new csvParse(_objectSpread(_objectSpread({}, options), {}, {
    columns: true
  }));
}

/**
 * @private
 */
export function serializeCSVStream(options) {
  return csvStringify(_objectSpread(_objectSpread({}, options), {}, {
    header: true
  }));
}
//# sourceMappingURL=data:application/json;charset=utf-8;base64,eyJ2ZXJzaW9uIjozLCJuYW1lcyI6WyJQYXJzZXIiLCJjc3ZQYXJzZSIsInBhcnNlIiwiY3N2UGFyc2VTeW5jIiwic3RyaW5naWZ5IiwiY3N2U3RyaW5naWZ5IiwiY3N2U3RyaW5naWZ5U3luYyIsInBhcnNlQ1NWIiwic3RyIiwib3B0aW9ucyIsIl9vYmplY3RTcHJlYWQiLCJjb2x1bW5zIiwidG9DU1YiLCJyZWNvcmRzIiwiaGVhZGVyIiwicGFyc2VDU1ZTdHJlYW0iLCJzZXJpYWxpemVDU1ZTdHJlYW0iXSwic291cmNlcyI6WyIuLi9zcmMvY3N2LnRzIl0sInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICpcbiAqL1xuaW1wb3J0IHsgVHJhbnNmb3JtIH0gZnJvbSAnc3RyZWFtJztcbmltcG9ydCB7IFBhcnNlciBhcyBjc3ZQYXJzZSB9IGZyb20gJ2Nzdi1wYXJzZSc7XG5pbXBvcnQgeyBPcHRpb25zIGFzIFBhcnNlT3B0cywgcGFyc2UgYXMgY3N2UGFyc2VTeW5jIH0gZnJvbSAnY3N2LXBhcnNlL3N5bmMnO1xuaW1wb3J0IHtcbiAgT3B0aW9ucyBhcyBTdHJpbmdpZnlPcHRzLFxuICBzdHJpbmdpZnkgYXMgY3N2U3RyaW5naWZ5LFxufSBmcm9tICdjc3Ytc3RyaW5naWZ5JztcbmltcG9ydCB7IHN0cmluZ2lmeSBhcyBjc3ZTdHJpbmdpZnlTeW5jIH0gZnJvbSAnY3N2LXN0cmluZ2lmeS9zeW5jJztcblxuLyoqXG4gKiBAcHJpdmF0ZVxuICovXG5leHBvcnQgZnVuY3Rpb24gcGFyc2VDU1Yoc3RyOiBzdHJpbmcsIG9wdGlvbnM/OiBQYXJzZU9wdHMpOiBPYmplY3RbXSB7XG4gIHJldHVybiBjc3ZQYXJzZVN5bmMoc3RyLCB7IC4uLm9wdGlvbnMsIGNvbHVtbnM6IHRydWUgfSk7XG59XG5cbi8qKlxuICogQHByaXZhdGVcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIHRvQ1NWKHJlY29yZHM6IE9iamVjdFtdLCBvcHRpb25zPzogU3RyaW5naWZ5T3B0cyk6IHN0cmluZyB7XG4gIHJldHVybiBjc3ZTdHJpbmdpZnlTeW5jKHJlY29yZHMsIHsgLi4ub3B0aW9ucywgaGVhZGVyOiB0cnVlIH0pO1xufVxuXG4vKipcbiAqIEBwcml2YXRlXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBwYXJzZUNTVlN0cmVhbShvcHRpb25zPzogUGFyc2VPcHRzKTogVHJhbnNmb3JtIHtcbiAgcmV0dXJuIG5ldyBjc3ZQYXJzZSh7IC4uLm9wdGlvbnMsIGNvbHVtbnM6IHRydWUgfSk7XG59XG5cbi8qKlxuICogQHByaXZhdGVcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIHNlcmlhbGl6ZUNTVlN0cmVhbShvcHRpb25zPzogU3RyaW5naWZ5T3B0cyk6IFRyYW5zZm9ybSB7XG4gIHJldHVybiBjc3ZTdHJpbmdpZnkoeyAuLi5vcHRpb25zLCBoZWFkZXI6IHRydWUgfSk7XG59XG4iXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7OztBQUFBO0FBQ0E7QUFDQTs7QUFFQSxTQUFTQSxNQUFNLElBQUlDLFFBQVEsUUFBUSxXQUFXO0FBQzlDLFNBQStCQyxLQUFLLElBQUlDLFlBQVksUUFBUSxnQkFBZ0I7QUFDNUUsU0FFRUMsU0FBUyxJQUFJQyxZQUFZLFFBQ3BCLGVBQWU7QUFDdEIsU0FBU0QsU0FBUyxJQUFJRSxnQkFBZ0IsUUFBUSxvQkFBb0I7O0FBRWxFO0FBQ0E7QUFDQTtBQUNBLE9BQU8sU0FBU0MsUUFBUUEsQ0FBQ0MsR0FBVyxFQUFFQyxPQUFtQixFQUFZO0VBQ25FLE9BQU9OLFlBQVksQ0FBQ0ssR0FBRyxFQUFBRSxhQUFBLENBQUFBLGFBQUEsS0FBT0QsT0FBTztJQUFFRSxPQUFPLEVBQUU7RUFBSSxFQUFFLENBQUM7QUFDekQ7O0FBRUE7QUFDQTtBQUNBO0FBQ0EsT0FBTyxTQUFTQyxLQUFLQSxDQUFDQyxPQUFpQixFQUFFSixPQUF1QixFQUFVO0VBQ3hFLE9BQU9ILGdCQUFnQixDQUFDTyxPQUFPLEVBQUFILGFBQUEsQ0FBQUEsYUFBQSxLQUFPRCxPQUFPO0lBQUVLLE1BQU0sRUFBRTtFQUFJLEVBQUUsQ0FBQztBQUNoRTs7QUFFQTtBQUNBO0FBQ0E7QUFDQSxPQUFPLFNBQVNDLGNBQWNBLENBQUNOLE9BQW1CLEVBQWE7RUFDN0QsT0FBTyxJQUFJUixRQUFRLENBQUFTLGFBQUEsQ0FBQUEsYUFBQSxLQUFNRCxPQUFPO0lBQUVFLE9BQU8sRUFBRTtFQUFJLEVBQUUsQ0FBQztBQUNwRDs7QUFFQTtBQUNBO0FBQ0E7QUFDQSxPQUFPLFNBQVNLLGtCQUFrQkEsQ0FBQ1AsT0FBdUIsRUFBYTtFQUNyRSxPQUFPSixZQUFZLENBQUFLLGFBQUEsQ0FBQUEsYUFBQSxLQUFNRCxPQUFPO0lBQUVLLE1BQU0sRUFBRTtFQUFJLEVBQUUsQ0FBQztBQUNuRCIsImlnbm9yZUxpc3QiOltdfQ==