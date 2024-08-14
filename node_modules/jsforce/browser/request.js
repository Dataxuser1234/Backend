import "core-js/modules/es.array.push.js";
import "core-js/modules/es.object.to-string.js";
import "core-js/modules/es.regexp.to-string.js";
import _Object$keys from "@babel/runtime-corejs3/core-js-stable/object/keys";
import _Object$getOwnPropertySymbols from "@babel/runtime-corejs3/core-js-stable/object/get-own-property-symbols";
import _filterInstanceProperty from "@babel/runtime-corejs3/core-js-stable/instance/filter";
import _Object$getOwnPropertyDescriptor from "@babel/runtime-corejs3/core-js-stable/object/get-own-property-descriptor";
import _forEachInstanceProperty from "@babel/runtime-corejs3/core-js-stable/instance/for-each";
import _Object$getOwnPropertyDescriptors from "@babel/runtime-corejs3/core-js-stable/object/get-own-property-descriptors";
import _Object$defineProperties from "@babel/runtime-corejs3/core-js-stable/object/define-properties";
import _Object$defineProperty from "@babel/runtime-corejs3/core-js-stable/object/define-property";
import _sliceInstanceProperty from "@babel/runtime-corejs3/core-js-stable/instance/slice";
import _Array$from from "@babel/runtime-corejs3/core-js-stable/array/from";
import _Symbol from "@babel/runtime-corejs3/core-js-stable/symbol";
import _getIteratorMethod from "@babel/runtime-corejs3/core-js/get-iterator-method";
import _Array$isArray from "@babel/runtime-corejs3/core-js-stable/array/is-array";
import _objectWithoutProperties from "@babel/runtime-corejs3/helpers/objectWithoutProperties";
import _defineProperty from "@babel/runtime-corejs3/helpers/defineProperty";
import _asyncToGenerator from "@babel/runtime-corejs3/helpers/asyncToGenerator";
var _excluded = ["url", "body"];
function _createForOfIteratorHelper(o, allowArrayLike) { var it = typeof _Symbol !== "undefined" && _getIteratorMethod(o) || o["@@iterator"]; if (!it) { if (_Array$isArray(o) || (it = _unsupportedIterableToArray(o)) || allowArrayLike && o && typeof o.length === "number") { if (it) o = it; var i = 0; var F = function F() {}; return { s: F, n: function n() { if (i >= o.length) return { done: true }; return { done: false, value: o[i++] }; }, e: function e(_e) { throw _e; }, f: F }; } throw new TypeError("Invalid attempt to iterate non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); } var normalCompletion = true, didErr = false, err; return { s: function s() { it = it.call(o); }, n: function n() { var step = it.next(); normalCompletion = step.done; return step; }, e: function e(_e2) { didErr = true; err = _e2; }, f: function f() { try { if (!normalCompletion && it.return != null) it.return(); } finally { if (didErr) throw err; } } }; }
function _unsupportedIterableToArray(o, minLen) { var _context8; if (!o) return; if (typeof o === "string") return _arrayLikeToArray(o, minLen); var n = _sliceInstanceProperty(_context8 = Object.prototype.toString.call(o)).call(_context8, 8, -1); if (n === "Object" && o.constructor) n = o.constructor.name; if (n === "Map" || n === "Set") return _Array$from(o); if (n === "Arguments" || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n)) return _arrayLikeToArray(o, minLen); }
function _arrayLikeToArray(arr, len) { if (len == null || len > arr.length) len = arr.length; for (var i = 0, arr2 = new Array(len); i < len; i++) arr2[i] = arr[i]; return arr2; }
import _regeneratorRuntime from "@babel/runtime-corejs3/regenerator";
import "core-js/modules/es.error.cause.js";
import "core-js/modules/es.function.name.js";
import "core-js/modules/es.regexp.exec.js";
import "core-js/modules/es.regexp.test.js";
import _Promise from "@babel/runtime-corejs3/core-js-stable/promise";
import _setTimeout from "@babel/runtime-corejs3/core-js-stable/set-timeout";
import _includesInstanceProperty from "@babel/runtime-corejs3/core-js-stable/instance/includes";
import _keysInstanceProperty from "@babel/runtime-corejs3/core-js-stable/instance/keys";
function ownKeys(e, r) { var t = _Object$keys(e); if (_Object$getOwnPropertySymbols) { var o = _Object$getOwnPropertySymbols(e); r && (o = _filterInstanceProperty(o).call(o, function (r) { return _Object$getOwnPropertyDescriptor(e, r).enumerable; })), t.push.apply(t, o); } return t; }
function _objectSpread(e) { for (var r = 1; r < arguments.length; r++) { var _context6, _context7; var t = null != arguments[r] ? arguments[r] : {}; r % 2 ? _forEachInstanceProperty(_context6 = ownKeys(Object(t), !0)).call(_context6, function (r) { _defineProperty(e, r, t[r]); }) : _Object$getOwnPropertyDescriptors ? _Object$defineProperties(e, _Object$getOwnPropertyDescriptors(t)) : _forEachInstanceProperty(_context7 = ownKeys(Object(t))).call(_context7, function (r) { _Object$defineProperty(e, r, _Object$getOwnPropertyDescriptor(t, r)); }); } return e; }
import { Readable } from 'stream';
import fetch, { Response } from 'node-fetch';
import AbortController from 'abort-controller';
import createHttpsProxyAgent from 'https-proxy-agent';
import { createHttpRequestHandlerStreams, executeWithTimeout, isRedirect, performRedirectRequest } from './request-helper';
import { getLogger } from './util/logger';
import is from '@sindresorhus/is';

/**
 *
 */
var defaults = {};

/**
 *
 */
export function setDefaults(defaults_) {
  defaults = defaults_;
}

/**
 *
 */
function startFetchRequest(_x, _x2, _x3, _x4, _x5) {
  return _startFetchRequest.apply(this, arguments);
}
/**
 *
 */
function _startFetchRequest() {
  _startFetchRequest = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee2(request, options, input, output, emitter) {
    var _options$retry$status, _options$retry, _options$retry$maxRet, _options$retry2, _options$retry$minTim, _options$retry3, _options$retry$timeou, _options$retry4, _options$retry$errorC, _options$retry5, _options$retry$method, _options$retry6, _context4;
    var counter,
      logger,
      httpProxy,
      followRedirect,
      agent,
      url,
      body,
      rrequest,
      controller,
      retryCount,
      retryOpts,
      shouldRetryRequest,
      fetchWithRetries,
      res,
      headers,
      _iterator,
      _step,
      headerName,
      response,
      _args2 = arguments;
    return _regeneratorRuntime.wrap(function _callee2$(_context5) {
      while (1) switch (_context5.prev = _context5.next) {
        case 0:
          counter = _args2.length > 5 && _args2[5] !== undefined ? _args2[5] : 0;
          logger = getLogger('fetch');
          httpProxy = options.httpProxy, followRedirect = options.followRedirect;
          agent = httpProxy ? createHttpsProxyAgent(httpProxy) : undefined;
          url = request.url, body = request.body, rrequest = _objectWithoutProperties(request, _excluded);
          controller = new AbortController();
          retryCount = 0;
          retryOpts = {
            statusCodes: (_options$retry$status = (_options$retry = options.retry) === null || _options$retry === void 0 ? void 0 : _options$retry.statusCodes) !== null && _options$retry$status !== void 0 ? _options$retry$status : [420, 429, 500, 502, 503, 504],
            maxRetries: (_options$retry$maxRet = (_options$retry2 = options.retry) === null || _options$retry2 === void 0 ? void 0 : _options$retry2.maxRetries) !== null && _options$retry$maxRet !== void 0 ? _options$retry$maxRet : 5,
            minTimeout: (_options$retry$minTim = (_options$retry3 = options.retry) === null || _options$retry3 === void 0 ? void 0 : _options$retry3.minTimeout) !== null && _options$retry$minTim !== void 0 ? _options$retry$minTim : 500,
            timeoutFactor: (_options$retry$timeou = (_options$retry4 = options.retry) === null || _options$retry4 === void 0 ? void 0 : _options$retry4.timeoutFactor) !== null && _options$retry$timeou !== void 0 ? _options$retry$timeou : 2,
            errorCodes: (_options$retry$errorC = (_options$retry5 = options.retry) === null || _options$retry5 === void 0 ? void 0 : _options$retry5.errorCodes) !== null && _options$retry$errorC !== void 0 ? _options$retry$errorC : ['ECONNRESET', 'ECONNREFUSED', 'ENOTFOUND', 'ENETDOWN', 'ENETUNREACH', 'EHOSTDOWN', 'UND_ERR_SOCKET', 'ETIMEDOUT', 'EPIPE'],
            methods: (_options$retry$method = (_options$retry6 = options.retry) === null || _options$retry6 === void 0 ? void 0 : _options$retry6.methods) !== null && _options$retry$method !== void 0 ? _options$retry$method : ['GET', 'PUT', 'HEAD', 'OPTIONS', 'DELETE']
          };
          shouldRetryRequest = function shouldRetryRequest(maxRetry, resOrErr) {
            var _context;
            if (!_includesInstanceProperty(_context = retryOpts.methods).call(_context, request.method)) return false;
            if (resOrErr instanceof Response) {
              var _context2;
              if (_includesInstanceProperty(_context2 = retryOpts.statusCodes).call(_context2, resOrErr.status)) {
                if (maxRetry === retryCount) {
                  var err = new Error('Request failed');
                  err.name = 'RequestRetryError';
                  throw err;
                } else {
                  return true;
                }
              }
              return false;
            } else {
              var _retryOpts$errorCodes;
              if (maxRetry === retryCount) return false;

              // only retry on operational errors
              // https://github.com/node-fetch/node-fetch/blob/2.x/ERROR-HANDLING.md#error-handling-with-node-fetch
              if (resOrErr.name != 'FetchError') return false;
              if (is.nodeStream(body) && Readable.isDisturbed(body)) {
                logger.debug('Body of type stream was read, unable to retry request.');
                return false;
              }
              if ('code' in resOrErr && resOrErr.code && retryOpts !== null && retryOpts !== void 0 && (_retryOpts$errorCodes = retryOpts.errorCodes) !== null && _retryOpts$errorCodes !== void 0 && _includesInstanceProperty(_retryOpts$errorCodes).call(_retryOpts$errorCodes, resOrErr.code)) return true;
              return false;
            }
          };
          fetchWithRetries = /*#__PURE__*/function () {
            var _ref = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee() {
              var maxRetry,
                fetchOpts,
                _res,
                error,
                _error,
                _args = arguments;
              return _regeneratorRuntime.wrap(function _callee$(_context3) {
                while (1) switch (_context3.prev = _context3.next) {
                  case 0:
                    maxRetry = _args.length > 0 && _args[0] !== undefined ? _args[0] : retryOpts === null || retryOpts === void 0 ? void 0 : retryOpts.maxRetries;
                    fetchOpts = _objectSpread(_objectSpread(_objectSpread({}, rrequest), input && /^(post|put|patch)$/i.test(request.method) ? {
                      body: input
                    } : {}), {}, {
                      redirect: 'manual',
                      // @ts-expect-error - differing types of signal? this started abruptly
                      signal: controller.signal,
                      agent: agent
                    });
                    _context3.prev = 2;
                    _context3.next = 5;
                    return fetch(url, fetchOpts);
                  case 5:
                    _res = _context3.sent;
                    if (!shouldRetryRequest(retryOpts.maxRetries, _res)) {
                      _context3.next = 16;
                      break;
                    }
                    logger.debug("retrying for the ".concat(retryCount + 1, " time"));
                    logger.debug('reason: statusCode match');
                    _context3.next = 11;
                    return sleep(retryCount === 0 ? retryOpts.minTimeout : retryOpts.minTimeout * Math.pow(retryOpts.timeoutFactor, retryCount));
                  case 11:
                    // NOTE: this event is only used by tests and will be removed at any time.
                    // jsforce may switch to node's fetch which doesn't emit this event on retries.
                    emitter.emit('retry', retryCount);
                    retryCount++;
                    _context3.next = 15;
                    return fetchWithRetries(maxRetry);
                  case 15:
                    return _context3.abrupt("return", _context3.sent);
                  case 16:
                    return _context3.abrupt("return", _res);
                  case 19:
                    _context3.prev = 19;
                    _context3.t0 = _context3["catch"](2);
                    logger.debug('Request failed');
                    error = _context3.t0; // request was canceled by consumer (AbortController), skip retry and rethrow.
                    if (!(error.name === 'AbortError')) {
                      _context3.next = 25;
                      break;
                    }
                    throw error;
                  case 25:
                    if (!shouldRetryRequest(retryOpts.maxRetries, error)) {
                      _context3.next = 33;
                      break;
                    }
                    logger.debug("retrying for the ".concat(retryCount + 1, " time"));
                    logger.debug("Error: ".concat(_context3.t0.message));
                    _context3.next = 30;
                    return sleep(retryCount === 0 ? retryOpts.minTimeout : retryOpts.minTimeout * Math.pow(retryOpts.timeoutFactor, retryCount));
                  case 30:
                    // NOTE: this event is only used by tests and will be removed at any time.
                    // jsforce may switch to node's fetch which doesn't emit this event on retries.
                    emitter.emit('retry', retryCount);
                    retryCount++;
                    return _context3.abrupt("return", fetchWithRetries(maxRetry));
                  case 33:
                    logger.debug('Skipping retry...');
                    if (!(maxRetry === retryCount)) {
                      _context3.next = 40;
                      break;
                    }
                    _error = new Error('Request failed', {
                      cause: _context3.t0
                    });
                    _error.name = 'RequestRetryError';
                    throw _error;
                  case 40:
                    throw _context3.t0;
                  case 41:
                  case "end":
                    return _context3.stop();
                }
              }, _callee, null, [[2, 19]]);
            }));
            return function fetchWithRetries() {
              return _ref.apply(this, arguments);
            };
          }();
          _context5.prev = 10;
          _context5.next = 13;
          return executeWithTimeout(fetchWithRetries, options.timeout, function () {
            return controller.abort();
          });
        case 13:
          res = _context5.sent;
          _context5.next = 20;
          break;
        case 16:
          _context5.prev = 16;
          _context5.t0 = _context5["catch"](10);
          emitter.emit('error', _context5.t0);
          return _context5.abrupt("return");
        case 20:
          headers = {};
          _iterator = _createForOfIteratorHelper(_keysInstanceProperty(_context4 = res.headers).call(_context4));
          try {
            for (_iterator.s(); !(_step = _iterator.n()).done;) {
              headerName = _step.value;
              headers[headerName.toLowerCase()] = res.headers.get(headerName);
            }
          } catch (err) {
            _iterator.e(err);
          } finally {
            _iterator.f();
          }
          response = {
            statusCode: res.status,
            headers: headers
          };
          if (!(followRedirect && isRedirect(response.statusCode))) {
            _context5.next = 27;
            break;
          }
          try {
            performRedirectRequest(request, response, followRedirect, counter, function (req) {
              return startFetchRequest(req, options, undefined, output, emitter, counter + 1);
            });
          } catch (err) {
            emitter.emit('error', err);
          }
          return _context5.abrupt("return");
        case 27:
          emitter.emit('response', response);
          res.body.pipe(output);
        case 29:
        case "end":
          return _context5.stop();
      }
    }, _callee2, null, [[10, 16]]);
  }));
  return _startFetchRequest.apply(this, arguments);
}
export default function request(req) {
  var options_ = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : {};
  var options = _objectSpread(_objectSpread({}, defaults), options_);
  var _createHttpRequestHan = createHttpRequestHandlerStreams(req, options),
    input = _createHttpRequestHan.input,
    output = _createHttpRequestHan.output,
    stream = _createHttpRequestHan.stream;
  startFetchRequest(req, options, input, output, stream);
  return stream;
}
var sleep = function sleep(ms) {
  return new _Promise(function (r) {
    return _setTimeout(r, ms);
  });
};
//# sourceMappingURL=data:application/json;charset=utf-8;base64,eyJ2ZXJzaW9uIjozLCJuYW1lcyI6WyJSZWFkYWJsZSIsImZldGNoIiwiUmVzcG9uc2UiLCJBYm9ydENvbnRyb2xsZXIiLCJjcmVhdGVIdHRwc1Byb3h5QWdlbnQiLCJjcmVhdGVIdHRwUmVxdWVzdEhhbmRsZXJTdHJlYW1zIiwiZXhlY3V0ZVdpdGhUaW1lb3V0IiwiaXNSZWRpcmVjdCIsInBlcmZvcm1SZWRpcmVjdFJlcXVlc3QiLCJnZXRMb2dnZXIiLCJpcyIsImRlZmF1bHRzIiwic2V0RGVmYXVsdHMiLCJkZWZhdWx0c18iLCJzdGFydEZldGNoUmVxdWVzdCIsIl94IiwiX3gyIiwiX3gzIiwiX3g0IiwiX3g1IiwiX3N0YXJ0RmV0Y2hSZXF1ZXN0IiwiYXBwbHkiLCJhcmd1bWVudHMiLCJfYXN5bmNUb0dlbmVyYXRvciIsIl9yZWdlbmVyYXRvclJ1bnRpbWUiLCJtYXJrIiwiX2NhbGxlZTIiLCJyZXF1ZXN0Iiwib3B0aW9ucyIsImlucHV0Iiwib3V0cHV0IiwiZW1pdHRlciIsIl9vcHRpb25zJHJldHJ5JHN0YXR1cyIsIl9vcHRpb25zJHJldHJ5IiwiX29wdGlvbnMkcmV0cnkkbWF4UmV0IiwiX29wdGlvbnMkcmV0cnkyIiwiX29wdGlvbnMkcmV0cnkkbWluVGltIiwiX29wdGlvbnMkcmV0cnkzIiwiX29wdGlvbnMkcmV0cnkkdGltZW91IiwiX29wdGlvbnMkcmV0cnk0IiwiX29wdGlvbnMkcmV0cnkkZXJyb3JDIiwiX29wdGlvbnMkcmV0cnk1IiwiX29wdGlvbnMkcmV0cnkkbWV0aG9kIiwiX29wdGlvbnMkcmV0cnk2IiwiX2NvbnRleHQ0IiwiY291bnRlciIsImxvZ2dlciIsImh0dHBQcm94eSIsImZvbGxvd1JlZGlyZWN0IiwiYWdlbnQiLCJ1cmwiLCJib2R5IiwicnJlcXVlc3QiLCJjb250cm9sbGVyIiwicmV0cnlDb3VudCIsInJldHJ5T3B0cyIsInNob3VsZFJldHJ5UmVxdWVzdCIsImZldGNoV2l0aFJldHJpZXMiLCJyZXMiLCJoZWFkZXJzIiwiX2l0ZXJhdG9yIiwiX3N0ZXAiLCJoZWFkZXJOYW1lIiwicmVzcG9uc2UiLCJfYXJnczIiLCJ3cmFwIiwiX2NhbGxlZTIkIiwiX2NvbnRleHQ1IiwicHJldiIsIm5leHQiLCJsZW5ndGgiLCJ1bmRlZmluZWQiLCJfb2JqZWN0V2l0aG91dFByb3BlcnRpZXMiLCJfZXhjbHVkZWQiLCJzdGF0dXNDb2RlcyIsInJldHJ5IiwibWF4UmV0cmllcyIsIm1pblRpbWVvdXQiLCJ0aW1lb3V0RmFjdG9yIiwiZXJyb3JDb2RlcyIsIm1ldGhvZHMiLCJtYXhSZXRyeSIsInJlc09yRXJyIiwiX2NvbnRleHQiLCJfaW5jbHVkZXNJbnN0YW5jZVByb3BlcnR5IiwiY2FsbCIsIm1ldGhvZCIsIl9jb250ZXh0MiIsInN0YXR1cyIsImVyciIsIkVycm9yIiwibmFtZSIsIl9yZXRyeU9wdHMkZXJyb3JDb2RlcyIsIm5vZGVTdHJlYW0iLCJpc0Rpc3R1cmJlZCIsImRlYnVnIiwiY29kZSIsIl9yZWYiLCJfY2FsbGVlIiwiZmV0Y2hPcHRzIiwiX3JlcyIsImVycm9yIiwiX2Vycm9yIiwiX2FyZ3MiLCJfY2FsbGVlJCIsIl9jb250ZXh0MyIsIl9vYmplY3RTcHJlYWQiLCJ0ZXN0IiwicmVkaXJlY3QiLCJzaWduYWwiLCJzZW50IiwiY29uY2F0Iiwic2xlZXAiLCJNYXRoIiwicG93IiwiZW1pdCIsImFicnVwdCIsInQwIiwibWVzc2FnZSIsImNhdXNlIiwic3RvcCIsInRpbWVvdXQiLCJhYm9ydCIsIl9jcmVhdGVGb3JPZkl0ZXJhdG9ySGVscGVyIiwiX2tleXNJbnN0YW5jZVByb3BlcnR5IiwicyIsIm4iLCJkb25lIiwidmFsdWUiLCJ0b0xvd2VyQ2FzZSIsImdldCIsImUiLCJmIiwic3RhdHVzQ29kZSIsInJlcSIsInBpcGUiLCJvcHRpb25zXyIsIl9jcmVhdGVIdHRwUmVxdWVzdEhhbiIsInN0cmVhbSIsIm1zIiwiX1Byb21pc2UiLCJyIiwiX3NldFRpbWVvdXQiXSwic291cmNlcyI6WyIuLi9zcmMvcmVxdWVzdC50cyJdLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQgeyBFdmVudEVtaXR0ZXIgfSBmcm9tICdldmVudHMnO1xuaW1wb3J0IHsgRHVwbGV4LCBSZWFkYWJsZSwgV3JpdGFibGUgfSBmcm9tICdzdHJlYW0nO1xuaW1wb3J0IGZldGNoLCB7IFJlc3BvbnNlLCBSZXF1ZXN0SW5pdCwgRmV0Y2hFcnJvciB9IGZyb20gJ25vZGUtZmV0Y2gnO1xuaW1wb3J0IEFib3J0Q29udHJvbGxlciBmcm9tICdhYm9ydC1jb250cm9sbGVyJztcbmltcG9ydCBjcmVhdGVIdHRwc1Byb3h5QWdlbnQgZnJvbSAnaHR0cHMtcHJveHktYWdlbnQnO1xuaW1wb3J0IHtcbiAgY3JlYXRlSHR0cFJlcXVlc3RIYW5kbGVyU3RyZWFtcyxcbiAgZXhlY3V0ZVdpdGhUaW1lb3V0LFxuICBpc1JlZGlyZWN0LFxuICBwZXJmb3JtUmVkaXJlY3RSZXF1ZXN0LFxufSBmcm9tICcuL3JlcXVlc3QtaGVscGVyJztcbmltcG9ydCB7IEh0dHBSZXF1ZXN0LCBIdHRwUmVxdWVzdE9wdGlvbnMgfSBmcm9tICcuL3R5cGVzJztcbmltcG9ydCB7IGdldExvZ2dlciB9IGZyb20gJy4vdXRpbC9sb2dnZXInO1xuaW1wb3J0IGlzIGZyb20gJ0BzaW5kcmVzb3JodXMvaXMnO1xuXG4vKipcbiAqXG4gKi9cbmxldCBkZWZhdWx0czogSHR0cFJlcXVlc3RPcHRpb25zID0ge307XG5cbi8qKlxuICpcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIHNldERlZmF1bHRzKGRlZmF1bHRzXzogSHR0cFJlcXVlc3RPcHRpb25zKSB7XG4gIGRlZmF1bHRzID0gZGVmYXVsdHNfO1xufVxuXG4vKipcbiAqXG4gKi9cbmFzeW5jIGZ1bmN0aW9uIHN0YXJ0RmV0Y2hSZXF1ZXN0KFxuICByZXF1ZXN0OiBIdHRwUmVxdWVzdCxcbiAgb3B0aW9uczogSHR0cFJlcXVlc3RPcHRpb25zLFxuICBpbnB1dDogUmVhZGFibGUgfCB1bmRlZmluZWQsXG4gIG91dHB1dDogV3JpdGFibGUsXG4gIGVtaXR0ZXI6IEV2ZW50RW1pdHRlcixcbiAgY291bnRlcjogbnVtYmVyID0gMCxcbikge1xuICBjb25zdCBsb2dnZXIgPSBnZXRMb2dnZXIoJ2ZldGNoJyk7XG4gIGNvbnN0IHsgaHR0cFByb3h5LCBmb2xsb3dSZWRpcmVjdCB9ID0gb3B0aW9ucztcbiAgY29uc3QgYWdlbnQgPSBodHRwUHJveHkgPyBjcmVhdGVIdHRwc1Byb3h5QWdlbnQoaHR0cFByb3h5KSA6IHVuZGVmaW5lZDtcbiAgY29uc3QgeyB1cmwsIGJvZHksIC4uLnJyZXF1ZXN0IH0gPSByZXF1ZXN0O1xuICBjb25zdCBjb250cm9sbGVyID0gbmV3IEFib3J0Q29udHJvbGxlcigpO1xuXG4gIGxldCByZXRyeUNvdW50ID0gMDtcblxuICBjb25zdCByZXRyeU9wdHM6IFJlcXVpcmVkPEh0dHBSZXF1ZXN0T3B0aW9uc1sncmV0cnknXT4gPSB7XG4gICAgc3RhdHVzQ29kZXM6IG9wdGlvbnMucmV0cnk/LnN0YXR1c0NvZGVzID8/IFs0MjAsIDQyOSwgNTAwLCA1MDIsIDUwMywgNTA0XSxcbiAgICBtYXhSZXRyaWVzOiBvcHRpb25zLnJldHJ5Py5tYXhSZXRyaWVzID8/IDUsXG4gICAgbWluVGltZW91dDogb3B0aW9ucy5yZXRyeT8ubWluVGltZW91dCA/PyA1MDAsXG4gICAgdGltZW91dEZhY3Rvcjogb3B0aW9ucy5yZXRyeT8udGltZW91dEZhY3RvciA/PyAyLFxuICAgIGVycm9yQ29kZXM6IG9wdGlvbnMucmV0cnk/LmVycm9yQ29kZXMgPz8gW1xuICAgICAgJ0VDT05OUkVTRVQnLFxuICAgICAgJ0VDT05OUkVGVVNFRCcsXG4gICAgICAnRU5PVEZPVU5EJyxcbiAgICAgICdFTkVURE9XTicsXG4gICAgICAnRU5FVFVOUkVBQ0gnLFxuICAgICAgJ0VIT1NURE9XTicsXG4gICAgICAnVU5EX0VSUl9TT0NLRVQnLFxuICAgICAgJ0VUSU1FRE9VVCcsXG4gICAgICAnRVBJUEUnLFxuICAgIF0sXG4gICAgbWV0aG9kczogb3B0aW9ucy5yZXRyeT8ubWV0aG9kcyA/PyBbXG4gICAgICAnR0VUJyxcbiAgICAgICdQVVQnLFxuICAgICAgJ0hFQUQnLFxuICAgICAgJ09QVElPTlMnLFxuICAgICAgJ0RFTEVURScsXG4gICAgXSxcbiAgfTtcblxuICBjb25zdCBzaG91bGRSZXRyeVJlcXVlc3QgPSAoXG4gICAgbWF4UmV0cnk6IG51bWJlcixcbiAgICByZXNPckVycjogUmVzcG9uc2UgfCBFcnJvciB8IEZldGNoRXJyb3IsXG4gICk6IGJvb2xlYW4gPT4ge1xuICAgIGlmICghcmV0cnlPcHRzLm1ldGhvZHMuaW5jbHVkZXMocmVxdWVzdC5tZXRob2QpKSByZXR1cm4gZmFsc2U7XG5cbiAgICBpZiAocmVzT3JFcnIgaW5zdGFuY2VvZiBSZXNwb25zZSkge1xuICAgICAgaWYgKHJldHJ5T3B0cy5zdGF0dXNDb2Rlcy5pbmNsdWRlcyhyZXNPckVyci5zdGF0dXMpKSB7XG4gICAgICAgIGlmIChtYXhSZXRyeSA9PT0gcmV0cnlDb3VudCkge1xuICAgICAgICAgIGNvbnN0IGVyciA9IG5ldyBFcnJvcignUmVxdWVzdCBmYWlsZWQnKTtcbiAgICAgICAgICBlcnIubmFtZSA9ICdSZXF1ZXN0UmV0cnlFcnJvcic7XG4gICAgICAgICAgdGhyb3cgZXJyO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIHJldHVybiB0cnVlO1xuICAgICAgICB9XG4gICAgICB9XG4gICAgICByZXR1cm4gZmFsc2U7XG4gICAgfSBlbHNlIHtcbiAgICAgIGlmIChtYXhSZXRyeSA9PT0gcmV0cnlDb3VudCkgcmV0dXJuIGZhbHNlO1xuXG4gICAgICAvLyBvbmx5IHJldHJ5IG9uIG9wZXJhdGlvbmFsIGVycm9yc1xuICAgICAgLy8gaHR0cHM6Ly9naXRodWIuY29tL25vZGUtZmV0Y2gvbm9kZS1mZXRjaC9ibG9iLzIueC9FUlJPUi1IQU5ETElORy5tZCNlcnJvci1oYW5kbGluZy13aXRoLW5vZGUtZmV0Y2hcbiAgICAgIGlmIChyZXNPckVyci5uYW1lICE9ICdGZXRjaEVycm9yJykgcmV0dXJuIGZhbHNlO1xuXG4gICAgICBpZiAoaXMubm9kZVN0cmVhbShib2R5KSAmJiBSZWFkYWJsZS5pc0Rpc3R1cmJlZChib2R5KSkge1xuICAgICAgICBsb2dnZXIuZGVidWcoJ0JvZHkgb2YgdHlwZSBzdHJlYW0gd2FzIHJlYWQsIHVuYWJsZSB0byByZXRyeSByZXF1ZXN0LicpO1xuICAgICAgICByZXR1cm4gZmFsc2U7XG4gICAgICB9XG5cbiAgICAgIGlmIChcbiAgICAgICAgJ2NvZGUnIGluIHJlc09yRXJyICYmXG4gICAgICAgIHJlc09yRXJyLmNvZGUgJiZcbiAgICAgICAgcmV0cnlPcHRzPy5lcnJvckNvZGVzPy5pbmNsdWRlcyhyZXNPckVyci5jb2RlKVxuICAgICAgKVxuICAgICAgICByZXR1cm4gdHJ1ZTtcblxuICAgICAgcmV0dXJuIGZhbHNlO1xuICAgIH1cbiAgfTtcblxuICBjb25zdCBmZXRjaFdpdGhSZXRyaWVzID0gYXN5bmMgKFxuICAgIG1heFJldHJ5ID0gcmV0cnlPcHRzPy5tYXhSZXRyaWVzLFxuICApOiBQcm9taXNlPFJlc3BvbnNlPiA9PiB7XG4gICAgY29uc3QgZmV0Y2hPcHRzOiBSZXF1ZXN0SW5pdCA9IHtcbiAgICAgIC4uLnJyZXF1ZXN0LFxuICAgICAgLi4uKGlucHV0ICYmIC9eKHBvc3R8cHV0fHBhdGNoKSQvaS50ZXN0KHJlcXVlc3QubWV0aG9kKVxuICAgICAgICA/IHsgYm9keTogaW5wdXQgfVxuICAgICAgICA6IHt9KSxcbiAgICAgIHJlZGlyZWN0OiAnbWFudWFsJyxcbiAgICAgIC8vIEB0cy1leHBlY3QtZXJyb3IgLSBkaWZmZXJpbmcgdHlwZXMgb2Ygc2lnbmFsPyB0aGlzIHN0YXJ0ZWQgYWJydXB0bHlcbiAgICAgIHNpZ25hbDogY29udHJvbGxlci5zaWduYWwsXG4gICAgICBhZ2VudCxcbiAgICB9O1xuXG4gICAgdHJ5IHtcbiAgICAgIGNvbnN0IHJlcyA9IGF3YWl0IGZldGNoKHVybCwgZmV0Y2hPcHRzKTtcbiAgICAgIGlmIChzaG91bGRSZXRyeVJlcXVlc3QocmV0cnlPcHRzLm1heFJldHJpZXMsIHJlcykpIHtcbiAgICAgICAgbG9nZ2VyLmRlYnVnKGByZXRyeWluZyBmb3IgdGhlICR7cmV0cnlDb3VudCArIDF9IHRpbWVgKTtcbiAgICAgICAgbG9nZ2VyLmRlYnVnKCdyZWFzb246IHN0YXR1c0NvZGUgbWF0Y2gnKTtcblxuICAgICAgICBhd2FpdCBzbGVlcChcbiAgICAgICAgICByZXRyeUNvdW50ID09PSAwXG4gICAgICAgICAgICA/IHJldHJ5T3B0cy5taW5UaW1lb3V0XG4gICAgICAgICAgICA6IHJldHJ5T3B0cy5taW5UaW1lb3V0ICogcmV0cnlPcHRzLnRpbWVvdXRGYWN0b3IgKiogcmV0cnlDb3VudCxcbiAgICAgICAgKTtcblxuICAgICAgICAvLyBOT1RFOiB0aGlzIGV2ZW50IGlzIG9ubHkgdXNlZCBieSB0ZXN0cyBhbmQgd2lsbCBiZSByZW1vdmVkIGF0IGFueSB0aW1lLlxuICAgICAgICAvLyBqc2ZvcmNlIG1heSBzd2l0Y2ggdG8gbm9kZSdzIGZldGNoIHdoaWNoIGRvZXNuJ3QgZW1pdCB0aGlzIGV2ZW50IG9uIHJldHJpZXMuXG4gICAgICAgIGVtaXR0ZXIuZW1pdCgncmV0cnknLCByZXRyeUNvdW50KTtcbiAgICAgICAgcmV0cnlDb3VudCsrO1xuXG4gICAgICAgIHJldHVybiBhd2FpdCBmZXRjaFdpdGhSZXRyaWVzKG1heFJldHJ5KTtcbiAgICAgIH1cbiAgICAgIC8vIHNob3VsZCB3ZSB0aHJvdyBoZXJlIGlmIHRoZSBtYXhSZXRyeSBhbHJlYWR5IGhhcHBlbmVkIGFuZCBzdGlsbCBnb3QgdGhlIHNhbWUgc3RhdHVzQ29kZT9cbiAgICAgIHJldHVybiByZXM7XG4gICAgfSBjYXRjaCAoZXJyKSB7XG4gICAgICBsb2dnZXIuZGVidWcoJ1JlcXVlc3QgZmFpbGVkJyk7XG4gICAgICBjb25zdCBlcnJvciA9IGVyciBhcyBFcnJvciB8IEZldGNoRXJyb3I7XG5cbiAgICAgIC8vIHJlcXVlc3Qgd2FzIGNhbmNlbGVkIGJ5IGNvbnN1bWVyIChBYm9ydENvbnRyb2xsZXIpLCBza2lwIHJldHJ5IGFuZCByZXRocm93LlxuICAgICAgaWYgKGVycm9yLm5hbWUgPT09ICdBYm9ydEVycm9yJykge1xuICAgICAgICB0aHJvdyBlcnJvcjtcbiAgICAgIH1cblxuICAgICAgaWYgKHNob3VsZFJldHJ5UmVxdWVzdChyZXRyeU9wdHMubWF4UmV0cmllcywgZXJyb3IpKSB7XG4gICAgICAgIGxvZ2dlci5kZWJ1ZyhgcmV0cnlpbmcgZm9yIHRoZSAke3JldHJ5Q291bnQgKyAxfSB0aW1lYCk7XG4gICAgICAgIGxvZ2dlci5kZWJ1ZyhgRXJyb3I6ICR7KGVyciBhcyBFcnJvcikubWVzc2FnZX1gKTtcblxuICAgICAgICBhd2FpdCBzbGVlcChcbiAgICAgICAgICByZXRyeUNvdW50ID09PSAwXG4gICAgICAgICAgICA/IHJldHJ5T3B0cy5taW5UaW1lb3V0XG4gICAgICAgICAgICA6IHJldHJ5T3B0cy5taW5UaW1lb3V0ICogcmV0cnlPcHRzLnRpbWVvdXRGYWN0b3IgKiogcmV0cnlDb3VudCxcbiAgICAgICAgKTtcblxuICAgICAgICAvLyBOT1RFOiB0aGlzIGV2ZW50IGlzIG9ubHkgdXNlZCBieSB0ZXN0cyBhbmQgd2lsbCBiZSByZW1vdmVkIGF0IGFueSB0aW1lLlxuICAgICAgICAvLyBqc2ZvcmNlIG1heSBzd2l0Y2ggdG8gbm9kZSdzIGZldGNoIHdoaWNoIGRvZXNuJ3QgZW1pdCB0aGlzIGV2ZW50IG9uIHJldHJpZXMuXG4gICAgICAgIGVtaXR0ZXIuZW1pdCgncmV0cnknLCByZXRyeUNvdW50KTtcbiAgICAgICAgcmV0cnlDb3VudCsrO1xuXG4gICAgICAgIHJldHVybiBmZXRjaFdpdGhSZXRyaWVzKG1heFJldHJ5KTtcbiAgICAgIH1cblxuICAgICAgbG9nZ2VyLmRlYnVnKCdTa2lwcGluZyByZXRyeS4uLicpO1xuXG4gICAgICBpZiAobWF4UmV0cnkgPT09IHJldHJ5Q291bnQpIHtcbiAgICAgICAgY29uc3QgZXJyb3IgPSBuZXcgRXJyb3IoJ1JlcXVlc3QgZmFpbGVkJywgeyBjYXVzZTogZXJyIH0pO1xuICAgICAgICBlcnJvci5uYW1lID0gJ1JlcXVlc3RSZXRyeUVycm9yJztcbiAgICAgICAgdGhyb3cgZXJyb3I7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICB0aHJvdyBlcnI7XG4gICAgICB9XG4gICAgfVxuICB9O1xuXG4gIGxldCByZXM6IFJlc3BvbnNlO1xuXG4gIHRyeSB7XG4gICAgcmVzID0gYXdhaXQgZXhlY3V0ZVdpdGhUaW1lb3V0KGZldGNoV2l0aFJldHJpZXMsIG9wdGlvbnMudGltZW91dCwgKCkgPT5cbiAgICAgIGNvbnRyb2xsZXIuYWJvcnQoKSxcbiAgICApO1xuICB9IGNhdGNoIChlcnIpIHtcbiAgICBlbWl0dGVyLmVtaXQoJ2Vycm9yJywgZXJyKTtcbiAgICByZXR1cm47XG4gIH1cbiAgY29uc3QgaGVhZGVyczogeyBba2V5OiBzdHJpbmddOiBhbnkgfSA9IHt9O1xuICBmb3IgKGNvbnN0IGhlYWRlck5hbWUgb2YgcmVzLmhlYWRlcnMua2V5cygpKSB7XG4gICAgaGVhZGVyc1toZWFkZXJOYW1lLnRvTG93ZXJDYXNlKCldID0gcmVzLmhlYWRlcnMuZ2V0KGhlYWRlck5hbWUpO1xuICB9XG4gIGNvbnN0IHJlc3BvbnNlID0ge1xuICAgIHN0YXR1c0NvZGU6IHJlcy5zdGF0dXMsXG4gICAgaGVhZGVycyxcbiAgfTtcbiAgaWYgKGZvbGxvd1JlZGlyZWN0ICYmIGlzUmVkaXJlY3QocmVzcG9uc2Uuc3RhdHVzQ29kZSkpIHtcbiAgICB0cnkge1xuICAgICAgcGVyZm9ybVJlZGlyZWN0UmVxdWVzdChcbiAgICAgICAgcmVxdWVzdCxcbiAgICAgICAgcmVzcG9uc2UsXG4gICAgICAgIGZvbGxvd1JlZGlyZWN0LFxuICAgICAgICBjb3VudGVyLFxuICAgICAgICAocmVxKSA9PlxuICAgICAgICAgIHN0YXJ0RmV0Y2hSZXF1ZXN0KFxuICAgICAgICAgICAgcmVxLFxuICAgICAgICAgICAgb3B0aW9ucyxcbiAgICAgICAgICAgIHVuZGVmaW5lZCxcbiAgICAgICAgICAgIG91dHB1dCxcbiAgICAgICAgICAgIGVtaXR0ZXIsXG4gICAgICAgICAgICBjb3VudGVyICsgMSxcbiAgICAgICAgICApLFxuICAgICAgKTtcbiAgICB9IGNhdGNoIChlcnIpIHtcbiAgICAgIGVtaXR0ZXIuZW1pdCgnZXJyb3InLCBlcnIpO1xuICAgIH1cbiAgICByZXR1cm47XG4gIH1cbiAgZW1pdHRlci5lbWl0KCdyZXNwb25zZScsIHJlc3BvbnNlKTtcbiAgcmVzLmJvZHkucGlwZShvdXRwdXQpO1xufVxuXG4vKipcbiAqXG4gKi9cbmV4cG9ydCBkZWZhdWx0IGZ1bmN0aW9uIHJlcXVlc3QoXG4gIHJlcTogSHR0cFJlcXVlc3QsXG4gIG9wdGlvbnNfOiBIdHRwUmVxdWVzdE9wdGlvbnMgPSB7fSxcbik6IER1cGxleCB7XG4gIGNvbnN0IG9wdGlvbnMgPSB7IC4uLmRlZmF1bHRzLCAuLi5vcHRpb25zXyB9O1xuICBjb25zdCB7IGlucHV0LCBvdXRwdXQsIHN0cmVhbSB9ID0gY3JlYXRlSHR0cFJlcXVlc3RIYW5kbGVyU3RyZWFtcyhcbiAgICByZXEsXG4gICAgb3B0aW9ucyxcbiAgKTtcbiAgc3RhcnRGZXRjaFJlcXVlc3QocmVxLCBvcHRpb25zLCBpbnB1dCwgb3V0cHV0LCBzdHJlYW0pO1xuICByZXR1cm4gc3RyZWFtO1xufVxuXG5jb25zdCBzbGVlcCA9IChtczogbnVtYmVyKSA9PiBuZXcgUHJvbWlzZSgocikgPT4gc2V0VGltZW91dChyLCBtcykpO1xuIl0sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7O0FBQ0EsU0FBaUJBLFFBQVEsUUFBa0IsUUFBUTtBQUNuRCxPQUFPQyxLQUFLLElBQUlDLFFBQVEsUUFBaUMsWUFBWTtBQUNyRSxPQUFPQyxlQUFlLE1BQU0sa0JBQWtCO0FBQzlDLE9BQU9DLHFCQUFxQixNQUFNLG1CQUFtQjtBQUNyRCxTQUNFQywrQkFBK0IsRUFDL0JDLGtCQUFrQixFQUNsQkMsVUFBVSxFQUNWQyxzQkFBc0IsUUFDakIsa0JBQWtCO0FB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