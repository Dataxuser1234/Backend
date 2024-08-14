import _asyncToGenerator from "@babel/runtime-corejs3/helpers/asyncToGenerator";
import _classCallCheck from "@babel/runtime-corejs3/helpers/classCallCheck";
import _createClass from "@babel/runtime-corejs3/helpers/createClass";
import _defineProperty from "@babel/runtime-corejs3/helpers/defineProperty";
import _objectWithoutProperties from "@babel/runtime-corejs3/helpers/objectWithoutProperties";
var _excluded = ["type", "attributes"];
import _regeneratorRuntime from "@babel/runtime-corejs3/regenerator";
function ownKeys(e, r) { var t = _Object$keys(e); if (_Object$getOwnPropertySymbols) { var o = _Object$getOwnPropertySymbols(e); r && (o = _filterInstanceProperty(o).call(o, function (r) { return _Object$getOwnPropertyDescriptor(e, r).enumerable; })), t.push.apply(t, o); } return t; }
function _objectSpread(e) { for (var r = 1; r < arguments.length; r++) { var _context12, _context13; var t = null != arguments[r] ? arguments[r] : {}; r % 2 ? _forEachInstanceProperty(_context12 = ownKeys(Object(t), !0)).call(_context12, function (r) { _defineProperty(e, r, t[r]); }) : _Object$getOwnPropertyDescriptors ? _Object$defineProperties(e, _Object$getOwnPropertyDescriptors(t)) : _forEachInstanceProperty(_context13 = ownKeys(Object(t))).call(_context13, function (r) { _Object$defineProperty(e, r, _Object$getOwnPropertyDescriptor(t, r)); }); } return e; }
function _createForOfIteratorHelper(o, allowArrayLike) { var it = typeof _Symbol !== "undefined" && _getIteratorMethod(o) || o["@@iterator"]; if (!it) { if (_Array$isArray(o) || (it = _unsupportedIterableToArray(o)) || allowArrayLike && o && typeof o.length === "number") { if (it) o = it; var i = 0; var F = function F() {}; return { s: F, n: function n() { if (i >= o.length) return { done: true }; return { done: false, value: o[i++] }; }, e: function e(_e) { throw _e; }, f: F }; } throw new TypeError("Invalid attempt to iterate non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); } var normalCompletion = true, didErr = false, err; return { s: function s() { it = it.call(o); }, n: function n() { var step = it.next(); normalCompletion = step.done; return step; }, e: function e(_e2) { didErr = true; err = _e2; }, f: function f() { try { if (!normalCompletion && it.return != null) it.return(); } finally { if (didErr) throw err; } } }; }
function _unsupportedIterableToArray(o, minLen) { var _context11; if (!o) return; if (typeof o === "string") return _arrayLikeToArray(o, minLen); var n = _sliceInstanceProperty(_context11 = Object.prototype.toString.call(o)).call(_context11, 8, -1); if (n === "Object" && o.constructor) n = o.constructor.name; if (n === "Map" || n === "Set") return _Array$from(o); if (n === "Arguments" || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n)) return _arrayLikeToArray(o, minLen); }
function _arrayLikeToArray(arr, len) { if (len == null || len > arr.length) len = arr.length; for (var i = 0, arr2 = new Array(len); i < len; i++) arr2[i] = arr[i]; return arr2; }
import "core-js/modules/es.error.cause.js";
import "core-js/modules/es.array.push.js";
import "core-js/modules/es.function.name.js";
import "core-js/modules/es.object.to-string.js";
import "core-js/modules/es.regexp.exec.js";
import "core-js/modules/es.regexp.test.js";
import "core-js/modules/es.regexp.to-string.js";
import _mapInstanceProperty from "@babel/runtime-corejs3/core-js-stable/instance/map";
import _Array$isArray from "@babel/runtime-corejs3/core-js-stable/array/is-array";
import _filterInstanceProperty from "@babel/runtime-corejs3/core-js-stable/instance/filter";
import _Object$keys from "@babel/runtime-corejs3/core-js-stable/object/keys";
import _concatInstanceProperty from "@babel/runtime-corejs3/core-js-stable/instance/concat";
import _sliceInstanceProperty from "@babel/runtime-corejs3/core-js-stable/instance/slice";
import _Array$from from "@babel/runtime-corejs3/core-js-stable/array/from";
import _Symbol from "@babel/runtime-corejs3/core-js-stable/symbol";
import _getIteratorMethod from "@babel/runtime-corejs3/core-js/get-iterator-method";
import _Object$getOwnPropertySymbols from "@babel/runtime-corejs3/core-js-stable/object/get-own-property-symbols";
import _Object$getOwnPropertyDescriptor from "@babel/runtime-corejs3/core-js-stable/object/get-own-property-descriptor";
import _forEachInstanceProperty from "@babel/runtime-corejs3/core-js-stable/instance/for-each";
import _Object$getOwnPropertyDescriptors from "@babel/runtime-corejs3/core-js-stable/object/get-own-property-descriptors";
import _Object$defineProperties from "@babel/runtime-corejs3/core-js-stable/object/define-properties";
import _Object$defineProperty from "@babel/runtime-corejs3/core-js-stable/object/define-property";
/**
 * @file Salesforce SOAP API
 * @author Shinichi Tomita <shinichi.tomita@gmail.com>
 */
import { registerModule } from '../jsforce';
import SOAP from '../soap';
import { ApiSchemas } from './soap/schema';

/**
 *
 */
function toSoapRecord(records) {
  var _context;
  return _mapInstanceProperty(_context = _Array$isArray(records) ? records : [records]).call(_context, function (record) {
    var _context2;
    var type = record.type,
      attributes = record.attributes,
      rec = _objectWithoutProperties(record, _excluded);
    var t = type || (attributes === null || attributes === void 0 ? void 0 : attributes.type);
    if (!t) {
      throw new Error('Given record is not including sObject type information');
    }
    var fieldsToNull = _filterInstanceProperty(_context2 = _Object$keys(rec)).call(_context2, function (field) {
      return record[field] === null;
    });
    var _iterator = _createForOfIteratorHelper(fieldsToNull),
      _step;
    try {
      for (_iterator.s(); !(_step = _iterator.n()).done;) {
        var field = _step.value;
        delete rec[field];
      }
    } catch (err) {
      _iterator.e(err);
    } finally {
      _iterator.f();
    }
    return fieldsToNull.length > 0 ? _objectSpread({
      type: t,
      fieldsToNull: fieldsToNull
    }, rec) : _objectSpread({
      type: t
    }, rec);
  });
}

/**
 * API class for Partner SOAP call
 */
export var SoapApi = /*#__PURE__*/function () {
  function SoapApi(conn) {
    _classCallCheck(this, SoapApi);
    this._conn = conn;
  }

  /**
   * Call SOAP Api (Partner) endpoint
   * @private
   */
  return _createClass(SoapApi, [{
    key: "_invoke",
    value: (function () {
      var _invoke2 = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee(method, message, schema) {
        var _context3;
        var soapEndpoint, res;
        return _regeneratorRuntime.wrap(function _callee$(_context4) {
          while (1) switch (_context4.prev = _context4.next) {
            case 0:
              soapEndpoint = new SOAP(this._conn, {
                xmlns: 'urn:partner.soap.sforce.com',
                endpointUrl: _concatInstanceProperty(_context3 = "".concat(this._conn.instanceUrl, "/services/Soap/u/")).call(_context3, this._conn.version)
              });
              _context4.next = 3;
              return soapEndpoint.invoke(method, message, schema ? {
                result: schema
              } : undefined, ApiSchemas);
            case 3:
              res = _context4.sent;
              return _context4.abrupt("return", res.result);
            case 5:
            case "end":
              return _context4.stop();
          }
        }, _callee, this);
      }));
      function _invoke(_x, _x2, _x3) {
        return _invoke2.apply(this, arguments);
      }
      return _invoke;
    }()
    /**
     * Converts a Lead into an Account, Contact, or (optionally) an Opportunity.
     */
    )
  }, {
    key: "convertLead",
    value: function () {
      var _convertLead = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee2(leadConverts) {
        var schema;
        return _regeneratorRuntime.wrap(function _callee2$(_context5) {
          while (1) switch (_context5.prev = _context5.next) {
            case 0:
              schema = _Array$isArray(leadConverts) ? [ApiSchemas.LeadConvertResult] : ApiSchemas.LeadConvertResult;
              return _context5.abrupt("return", this._invoke('convertLead', {
                leadConverts: leadConverts
              }, schema));
            case 2:
            case "end":
              return _context5.stop();
          }
        }, _callee2, this);
      }));
      function convertLead(_x4) {
        return _convertLead.apply(this, arguments);
      }
      return convertLead;
    }()
    /**
     * Merge up to three records into one
     */
  }, {
    key: "merge",
    value: function () {
      var _merge = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee3(mergeRequests) {
        var schema;
        return _regeneratorRuntime.wrap(function _callee3$(_context6) {
          while (1) switch (_context6.prev = _context6.next) {
            case 0:
              schema = _Array$isArray(mergeRequests) ? [ApiSchemas.MergeResult] : ApiSchemas.MergeResult;
              return _context6.abrupt("return", this._invoke('merge', {
                mergeRequests: mergeRequests
              }, schema));
            case 2:
            case "end":
              return _context6.stop();
          }
        }, _callee3, this);
      }));
      function merge(_x5) {
        return _merge.apply(this, arguments);
      }
      return merge;
    }()
    /**
     * Delete records from the recycle bin immediately
     */
  }, {
    key: "emptyRecycleBin",
    value: (function () {
      var _emptyRecycleBin = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee4(ids) {
        return _regeneratorRuntime.wrap(function _callee4$(_context7) {
          while (1) switch (_context7.prev = _context7.next) {
            case 0:
              return _context7.abrupt("return", this._invoke('emptyRecycleBin', {
                ids: ids
              }, [ApiSchemas.EmptyRecycleBinResult]));
            case 1:
            case "end":
              return _context7.stop();
          }
        }, _callee4, this);
      }));
      function emptyRecycleBin(_x6) {
        return _emptyRecycleBin.apply(this, arguments);
      }
      return emptyRecycleBin;
    }()
    /**
     * Returns information about the standard and custom apps available to the logged-in user
     */
    )
  }, {
    key: "describeTabs",
    value: (function () {
      var _describeTabs = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee5() {
        return _regeneratorRuntime.wrap(function _callee5$(_context8) {
          while (1) switch (_context8.prev = _context8.next) {
            case 0:
              return _context8.abrupt("return", this._invoke('describeTabs', {}, [ApiSchemas.DescribeTabSetResult]));
            case 1:
            case "end":
              return _context8.stop();
          }
        }, _callee5, this);
      }));
      function describeTabs() {
        return _describeTabs.apply(this, arguments);
      }
      return describeTabs;
    }()
    /**
     * Retrieves the current system timestamp (Coordinated Universal Time (UTC) time zone) from the API
     */
    )
  }, {
    key: "getServerTimestamp",
    value: (function () {
      var _getServerTimestamp = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee6() {
        return _regeneratorRuntime.wrap(function _callee6$(_context9) {
          while (1) switch (_context9.prev = _context9.next) {
            case 0:
              return _context9.abrupt("return", this._invoke('getServerTimestamp', {}, ApiSchemas.GetServerTimestampResult));
            case 1:
            case "end":
              return _context9.stop();
          }
        }, _callee6, this);
      }));
      function getServerTimestamp() {
        return _getServerTimestamp.apply(this, arguments);
      }
      return getServerTimestamp;
    }()
    /**
     * Retrieves personal information for the user associated with the current session
     */
    )
  }, {
    key: "getUserInfo",
    value: (function () {
      var _getUserInfo = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee7() {
        return _regeneratorRuntime.wrap(function _callee7$(_context10) {
          while (1) switch (_context10.prev = _context10.next) {
            case 0:
              return _context10.abrupt("return", this._invoke('getUserInfo', {}, ApiSchemas.GetUserInfoResult));
            case 1:
            case "end":
              return _context10.stop();
          }
        }, _callee7, this);
      }));
      function getUserInfo() {
        return _getUserInfo.apply(this, arguments);
      }
      return getUserInfo;
    }()
    /**
     * Sets the specified user’s password to the specified value
     */
    )
  }, {
    key: "setPassword",
    value: function setPassword(userId, password) {
      return this._invoke('setPassword', {
        userId: userId,
        password: password
      }, 'string');
    }

    /**
     * Resets the specified user’s password
     */
  }, {
    key: "resetPassword",
    value: function resetPassword(userId) {
      return this._invoke('resetPassword', {
        userId: userId
      }, ApiSchemas.ResetPasswordResult);
    }

    /**
     * Adds one or more new records to your organization’s data
     */
  }, {
    key: "create",
    value: function create(sObjects) {
      var schema = _Array$isArray(sObjects) ? [ApiSchemas.SaveResult] : ApiSchemas.SaveResult;
      var args = {
        '@xmlns': 'urn:partner.soap.sforce.com',
        '@xmlns:ns1': 'sobject.partner.soap.sforce.com',
        'ns1:sObjects': toSoapRecord(sObjects)
      };
      return this._invoke('create', args, schema);
    }

    /**
     * Updates one or more existing records in your organization’s data.
     */
  }, {
    key: "update",
    value: function update(sObjects) {
      var schema = _Array$isArray(sObjects) ? [ApiSchemas.SaveResult] : ApiSchemas.SaveResult;
      var args = {
        '@xmlns': 'urn:partner.soap.sforce.com',
        '@xmlns:ns1': 'sobject.partner.soap.sforce.com',
        'ns1:sObjects': toSoapRecord(sObjects)
      };
      return this._invoke('update', args, schema);
    }

    /**
     * Creates new records and updates existing records in your organization’s data.
     */
  }, {
    key: "upsert",
    value: function upsert(externalIdFieldName, sObjects) {
      var schema = _Array$isArray(sObjects) ? [ApiSchemas.UpsertResult] : ApiSchemas.UpsertResult;
      var args = {
        '@xmlns': 'urn:partner.soap.sforce.com',
        '@xmlns:ns1': 'sobject.partner.soap.sforce.com',
        'ns1:externalIDFieldName': externalIdFieldName,
        'ns1:sObjects': toSoapRecord(sObjects)
      };
      return this._invoke('upsert', args, schema);
    }

    /**
     * Deletes one or more records from your organization’s data
     */
  }, {
    key: "delete",
    value: function _delete(ids) {
      var schema = _Array$isArray(ids) ? [ApiSchemas.DeleteResult] : ApiSchemas.DeleteResult;
      var args = {
        '@xmlns': 'urn:partner.soap.sforce.com',
        '@xmlns:ns1': 'sobject.partner.soap.sforce.com',
        'ns1:ids': ids
      };
      return this._invoke('delete', args, schema);
    }
  }]);
}();

/*--------------------------------------------*/
/*
 * Register hook in connection instantiation for dynamically adding this API module features
 */
registerModule('soap', function (conn) {
  return new SoapApi(conn);
});
export default SoapApi;
//# sourceMappingURL=data:application/json;charset=utf-8;base64,eyJ2ZXJzaW9uIjozLCJuYW1lcyI6WyJyZWdpc3Rlck1vZHVsZSIsIlNPQVAiLCJBcGlTY2hlbWFzIiwidG9Tb2FwUmVjb3JkIiwicmVjb3JkcyIsIl9jb250ZXh0IiwiX21hcEluc3RhbmNlUHJvcGVydHkiLCJfQXJyYXkkaXNBcnJheSIsImNhbGwiLCJyZWNvcmQiLCJfY29udGV4dDIiLCJ0eXBlIiwiYXR0cmlidXRlcyIsInJlYyIsIl9vYmplY3RXaXRob3V0UHJvcGVydGllcyIsIl9leGNsdWRlZCIsInQiLCJFcnJvciIsImZpZWxkc1RvTnVsbCIsIl9maWx0ZXJJbnN0YW5jZVByb3BlcnR5IiwiX09iamVjdCRrZXlzIiwiZmllbGQiLCJfaXRlcmF0b3IiLCJfY3JlYXRlRm9yT2ZJdGVyYXRvckhlbHBlciIsIl9zdGVwIiwicyIsIm4iLCJkb25lIiwidmFsdWUiLCJlcnIiLCJlIiwiZiIsImxlbmd0aCIsIl9vYmplY3RTcHJlYWQiLCJTb2FwQXBpIiwiY29ubiIsIl9jbGFzc0NhbGxDaGVjayIsIl9jb25uIiwiX2NyZWF0ZUNsYXNzIiwia2V5IiwiX2ludm9rZTIiLCJfYXN5bmNUb0dlbmVyYXRvciIsIl9yZWdlbmVyYXRvclJ1bnRpbWUiLCJtYXJrIiwiX2NhbGxlZSIsIm1ldGhvZCIsIm1lc3NhZ2UiLCJzY2hlbWEiLCJfY29udGV4dDMiLCJzb2FwRW5kcG9pbnQiLCJyZXMiLCJ3cmFwIiwiX2NhbGxlZSQiLCJfY29udGV4dDQiLCJwcmV2IiwibmV4dCIsInhtbG5zIiwiZW5kcG9pbnRVcmwiLCJfY29uY2F0SW5zdGFuY2VQcm9wZXJ0eSIsImNvbmNhdCIsImluc3RhbmNlVXJsIiwidmVyc2lvbiIsImludm9rZSIsInJlc3VsdCIsInVuZGVmaW5lZCIsInNlbnQiLCJhYnJ1cHQiLCJzdG9wIiwiX2ludm9rZSIsIl94IiwiX3gyIiwiX3gzIiwiYXBwbHkiLCJhcmd1bWVudHMiLCJfY29udmVydExlYWQiLCJfY2FsbGVlMiIsImxlYWRDb252ZXJ0cyIsIl9jYWxsZWUyJCIsIl9jb250ZXh0NSIsIkxlYWRDb252ZXJ0UmVzdWx0IiwiY29udmVydExlYWQiLCJfeDQiLCJfbWVyZ2UiLCJfY2FsbGVlMyIsIm1lcmdlUmVxdWVzdHMiLCJfY2FsbGVlMyQiLCJfY29udGV4dDYiLCJNZXJnZVJlc3VsdCIsIm1lcmdlIiwiX3g1IiwiX2VtcHR5UmVjeWNsZUJpbiIsIl9jYWxsZWU0IiwiaWRzIiwiX2NhbGxlZTQkIiwiX2NvbnRleHQ3IiwiRW1wdHlSZWN5Y2xlQmluUmVzdWx0IiwiZW1wdHlSZWN5Y2xlQmluIiwiX3g2IiwiX2Rlc2NyaWJlVGFicyIsIl9jYWxsZWU1IiwiX2NhbGxlZTUkIiwiX2NvbnRleHQ4IiwiRGVzY3JpYmVUYWJTZXRSZXN1bHQiLCJkZXNjcmliZVRhYnMiLCJfZ2V0U2VydmVyVGltZXN0YW1wIiwiX2NhbGxlZTYiLCJfY2FsbGVlNiQiLCJfY29udGV4dDkiLCJHZXRTZXJ2ZXJUaW1lc3RhbXBSZXN1bHQiLCJnZXRTZXJ2ZXJUaW1lc3RhbXAiLCJfZ2V0VXNlckluZm8iLCJfY2FsbGVlNyIsIl9jYWxsZWU3JCIsIl9jb250ZXh0MTAiLCJHZXRVc2VySW5mb1Jlc3VsdCIsImdldFVzZXJJbmZvIiwic2V0UGFzc3dvcmQiLCJ1c2VySWQiLCJwYXNzd29yZCIsInJlc2V0UGFzc3dvcmQiLCJSZXNldFBhc3N3b3JkUmVzdWx0IiwiY3JlYXRlIiwic09iamVjdHMiLCJTYXZlUmVzdWx0IiwiYXJncyIsInVwZGF0ZSIsInVwc2VydCIsImV4dGVybmFsSWRGaWVsZE5hbWUiLCJVcHNlcnRSZXN1bHQiLCJfZGVsZXRlIiwiRGVsZXRlUmVzdWx0Il0sInNvdXJjZXMiOlsiLi4vLi4vc3JjL2FwaS9zb2FwLnRzIl0sInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogQGZpbGUgU2FsZXNmb3JjZSBTT0FQIEFQSVxuICogQGF1dGhvciBTaGluaWNoaSBUb21pdGEgPHNoaW5pY2hpLnRvbWl0YUBnbWFpbC5jb20+XG4gKi9cbmltcG9ydCB7IHJlZ2lzdGVyTW9kdWxlIH0gZnJvbSAnLi4vanNmb3JjZSc7XG5pbXBvcnQgQ29ubmVjdGlvbiBmcm9tICcuLi9jb25uZWN0aW9uJztcbmltcG9ydCBTT0FQIGZyb20gJy4uL3NvYXAnO1xuaW1wb3J0IHsgU2NoZW1hLCBSZWNvcmQsIFNvYXBTY2hlbWFEZWYsIFNvYXBTY2hlbWEgfSBmcm9tICcuLi90eXBlcyc7XG5pbXBvcnQge1xuICBBcGlTY2hlbWFzLFxuICBMZWFkQ29udmVydCxcbiAgTGVhZENvbnZlcnRSZXN1bHQsXG4gIE1lcmdlUmVxdWVzdCxcbiAgTWVyZ2VSZXN1bHQsXG4gIEVtcHR5UmVjeWNsZUJpblJlc3VsdCxcbiAgRGVzY3JpYmVUYWJTZXRSZXN1bHQsXG4gIEdldFNlcnZlclRpbWVzdGFtcFJlc3VsdCxcbiAgR2V0VXNlckluZm9SZXN1bHQsXG4gIFJlc2V0UGFzc3dvcmRSZXN1bHQsXG4gIFNhdmVSZXN1bHQsXG4gIFVwc2VydFJlc3VsdCxcbiAgRGVsZXRlUmVzdWx0LFxufSBmcm9tICcuL3NvYXAvc2NoZW1hJztcblxuLyoqXG4gKlxuICovXG5mdW5jdGlvbiB0b1NvYXBSZWNvcmQocmVjb3JkczogUmVjb3JkIHwgUmVjb3JkW10pOiBSZWNvcmQgfCBSZWNvcmRbXSB7XG4gIHJldHVybiAoQXJyYXkuaXNBcnJheShyZWNvcmRzKSA/IHJlY29yZHMgOiBbcmVjb3Jkc10pLm1hcCgocmVjb3JkKSA9PiB7XG4gICAgY29uc3QgeyB0eXBlLCBhdHRyaWJ1dGVzLCAuLi5yZWMgfSA9IHJlY29yZDtcbiAgICBjb25zdCB0ID0gdHlwZSB8fCBhdHRyaWJ1dGVzPy50eXBlO1xuICAgIGlmICghdCkge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKCdHaXZlbiByZWNvcmQgaXMgbm90IGluY2x1ZGluZyBzT2JqZWN0IHR5cGUgaW5mb3JtYXRpb24nKTtcbiAgICB9XG4gICAgY29uc3QgZmllbGRzVG9OdWxsID0gT2JqZWN0LmtleXMocmVjKS5maWx0ZXIoXG4gICAgICAoZmllbGQpID0+IHJlY29yZFtmaWVsZF0gPT09IG51bGwsXG4gICAgKTtcbiAgICBmb3IgKGNvbnN0IGZpZWxkIG9mIGZpZWxkc1RvTnVsbCkge1xuICAgICAgZGVsZXRlIHJlY1tmaWVsZF07XG4gICAgfVxuICAgIHJldHVybiBmaWVsZHNUb051bGwubGVuZ3RoID4gMFxuICAgICAgPyB7IHR5cGU6IHQsIGZpZWxkc1RvTnVsbCwgLi4ucmVjIH1cbiAgICAgIDogeyB0eXBlOiB0LCAuLi5yZWMgfTtcbiAgfSk7XG59XG5cbi8qKlxuICogQVBJIGNsYXNzIGZvciBQYXJ0bmVyIFNPQVAgY2FsbFxuICovXG5leHBvcnQgY2xhc3MgU29hcEFwaTxTIGV4dGVuZHMgU2NoZW1hPiB7XG4gIF9jb25uOiBDb25uZWN0aW9uPFM+O1xuXG4gIGNvbnN0cnVjdG9yKGNvbm46IENvbm5lY3Rpb248Uz4pIHtcbiAgICB0aGlzLl9jb25uID0gY29ubjtcbiAgfVxuXG4gIC8qKlxuICAgKiBDYWxsIFNPQVAgQXBpIChQYXJ0bmVyKSBlbmRwb2ludFxuICAgKiBAcHJpdmF0ZVxuICAgKi9cbiAgYXN5bmMgX2ludm9rZShcbiAgICBtZXRob2Q6IHN0cmluZyxcbiAgICBtZXNzYWdlOiBvYmplY3QsXG4gICAgc2NoZW1hOiBTb2FwU2NoZW1hIHwgU29hcFNjaGVtYURlZixcbiAgKSB7XG4gICAgY29uc3Qgc29hcEVuZHBvaW50ID0gbmV3IFNPQVAodGhpcy5fY29ubiwge1xuICAgICAgeG1sbnM6ICd1cm46cGFydG5lci5zb2FwLnNmb3JjZS5jb20nLFxuICAgICAgZW5kcG9pbnRVcmw6IGAke3RoaXMuX2Nvbm4uaW5zdGFuY2VVcmx9L3NlcnZpY2VzL1NvYXAvdS8ke3RoaXMuX2Nvbm4udmVyc2lvbn1gLFxuICAgIH0pO1xuICAgIGNvbnN0IHJlcyA9IGF3YWl0IHNvYXBFbmRwb2ludC5pbnZva2UoXG4gICAgICBtZXRob2QsXG4gICAgICBtZXNzYWdlLFxuICAgICAgc2NoZW1hID8gKHsgcmVzdWx0OiBzY2hlbWEgfSBhcyBTb2FwU2NoZW1hKSA6IHVuZGVmaW5lZCxcbiAgICAgIEFwaVNjaGVtYXMsXG4gICAgKTtcbiAgICByZXR1cm4gcmVzLnJlc3VsdDtcbiAgfVxuXG4gIC8qKlxuICAgKiBDb252ZXJ0cyBhIExlYWQgaW50byBhbiBBY2NvdW50LCBDb250YWN0LCBvciAob3B0aW9uYWxseSkgYW4gT3Bwb3J0dW5pdHkuXG4gICAqL1xuICBjb252ZXJ0TGVhZChcbiAgICBsZWFkQ29udmVydHM6IEFycmF5PFBhcnRpYWw8TGVhZENvbnZlcnQ+PixcbiAgKTogUHJvbWlzZTxMZWFkQ29udmVydFJlc3VsdFtdPjtcbiAgY29udmVydExlYWQobGVhZENvbnZlcnQ6IFBhcnRpYWw8TGVhZENvbnZlcnQ+KTogUHJvbWlzZTxMZWFkQ29udmVydFJlc3VsdD47XG4gIGNvbnZlcnRMZWFkKFxuICAgIGxlYWRDb252ZXJ0OiBQYXJ0aWFsPExlYWRDb252ZXJ0PiB8IEFycmF5PFBhcnRpYWw8TGVhZENvbnZlcnQ+PixcbiAgKTogUHJvbWlzZTxMZWFkQ29udmVydFJlc3VsdCB8IExlYWRDb252ZXJ0UmVzdWx0W10+O1xuICBhc3luYyBjb252ZXJ0TGVhZChcbiAgICBsZWFkQ29udmVydHM6IFBhcnRpYWw8TGVhZENvbnZlcnQ+IHwgQXJyYXk8UGFydGlhbDxMZWFkQ29udmVydD4+LFxuICApIHtcbiAgICBjb25zdCBzY2hlbWEgPSBBcnJheS5pc0FycmF5KGxlYWRDb252ZXJ0cylcbiAgICAgID8gW0FwaVNjaGVtYXMuTGVhZENvbnZlcnRSZXN1bHRdXG4gICAgICA6IEFwaVNjaGVtYXMuTGVhZENvbnZlcnRSZXN1bHQ7XG4gICAgcmV0dXJuIHRoaXMuX2ludm9rZSgnY29udmVydExlYWQnLCB7IGxlYWRDb252ZXJ0cyB9LCBzY2hlbWEpO1xuICB9XG5cbiAgLyoqXG4gICAqIE1lcmdlIHVwIHRvIHRocmVlIHJlY29yZHMgaW50byBvbmVcbiAgICovXG4gIG1lcmdlKG1lcmdlUmVxdWVzdHM6IEFycmF5PFBhcnRpYWw8TWVyZ2VSZXF1ZXN0Pj4pOiBQcm9taXNlPE1lcmdlUmVzdWx0W10+O1xuICBtZXJnZShtZXJnZVJlcXVlc3Q6IFBhcnRpYWw8TWVyZ2VSZXF1ZXN0Pik6IFByb21pc2U8TWVyZ2VSZXN1bHQ+O1xuICBtZXJnZShcbiAgICBtZXJnZVJlcXVlc3Q6IFBhcnRpYWw8TWVyZ2VSZXF1ZXN0PiB8IEFycmF5PFBhcnRpYWw8TWVyZ2VSZXF1ZXN0Pj4sXG4gICk6IFByb21pc2U8TWVyZ2VSZXN1bHQgfCBNZXJnZVJlc3VsdFtdPjtcbiAgYXN5bmMgbWVyZ2UobWVyZ2VSZXF1ZXN0czogUGFydGlhbDxNZXJnZVJlcXVlc3Q+IHwgQXJyYXk8UGFydGlhbDxNZXJnZVJlcXVlc3Q+Pikge1xuICAgIGNvbnN0IHNjaGVtYSA9IEFycmF5LmlzQXJyYXkobWVyZ2VSZXF1ZXN0cylcbiAgICAgID8gW0FwaVNjaGVtYXMuTWVyZ2VSZXN1bHRdXG4gICAgICA6IEFwaVNjaGVtYXMuTWVyZ2VSZXN1bHQ7XG4gICAgcmV0dXJuIHRoaXMuX2ludm9rZSgnbWVyZ2UnLCB7IG1lcmdlUmVxdWVzdHMgfSwgc2NoZW1hKTtcbiAgfVxuXG4gIC8qKlxuICAgKiBEZWxldGUgcmVjb3JkcyBmcm9tIHRoZSByZWN5Y2xlIGJpbiBpbW1lZGlhdGVseVxuICAgKi9cbiAgYXN5bmMgZW1wdHlSZWN5Y2xlQmluKGlkczogc3RyaW5nW10pOiBQcm9taXNlPEVtcHR5UmVjeWNsZUJpblJlc3VsdD4ge1xuICAgIHJldHVybiB0aGlzLl9pbnZva2UoJ2VtcHR5UmVjeWNsZUJpbicsIHsgaWRzIH0sIFtcbiAgICAgIEFwaVNjaGVtYXMuRW1wdHlSZWN5Y2xlQmluUmVzdWx0LFxuICAgIF0pO1xuICB9XG5cbiAgLyoqXG4gICAqIFJldHVybnMgaW5mb3JtYXRpb24gYWJvdXQgdGhlIHN0YW5kYXJkIGFuZCBjdXN0b20gYXBwcyBhdmFpbGFibGUgdG8gdGhlIGxvZ2dlZC1pbiB1c2VyXG4gICAqL1xuICBhc3luYyBkZXNjcmliZVRhYnMoKTogUHJvbWlzZTxEZXNjcmliZVRhYlNldFJlc3VsdFtdPiB7XG4gICAgcmV0dXJuIHRoaXMuX2ludm9rZSgnZGVzY3JpYmVUYWJzJywge30sIFtBcGlTY2hlbWFzLkRlc2NyaWJlVGFiU2V0UmVzdWx0XSk7XG4gIH1cblxuICAvKipcbiAgICogUmV0cmlldmVzIHRoZSBjdXJyZW50IHN5c3RlbSB0aW1lc3RhbXAgKENvb3JkaW5hdGVkIFVuaXZlcnNhbCBUaW1lIChVVEMpIHRpbWUgem9uZSkgZnJvbSB0aGUgQVBJXG4gICAqL1xuICBhc3luYyBnZXRTZXJ2ZXJUaW1lc3RhbXAoKTogUHJvbWlzZTxHZXRTZXJ2ZXJUaW1lc3RhbXBSZXN1bHQ+IHtcbiAgICByZXR1cm4gdGhpcy5faW52b2tlKFxuICAgICAgJ2dldFNlcnZlclRpbWVzdGFtcCcsXG4gICAgICB7fSxcbiAgICAgIEFwaVNjaGVtYXMuR2V0U2VydmVyVGltZXN0YW1wUmVzdWx0LFxuICAgICk7XG4gIH1cblxuICAvKipcbiAgICogUmV0cmlldmVzIHBlcnNvbmFsIGluZm9ybWF0aW9uIGZvciB0aGUgdXNlciBhc3NvY2lhdGVkIHdpdGggdGhlIGN1cnJlbnQgc2Vzc2lvblxuICAgKi9cbiAgYXN5bmMgZ2V0VXNlckluZm8oKTogUHJvbWlzZTxHZXRVc2VySW5mb1Jlc3VsdD4ge1xuICAgIHJldHVybiB0aGlzLl9pbnZva2UoJ2dldFVzZXJJbmZvJywge30sIEFwaVNjaGVtYXMuR2V0VXNlckluZm9SZXN1bHQpO1xuICB9XG5cbiAgLyoqXG4gICAqIFNldHMgdGhlIHNwZWNpZmllZCB1c2Vy4oCZcyBwYXNzd29yZCB0byB0aGUgc3BlY2lmaWVkIHZhbHVlXG4gICAqL1xuICBzZXRQYXNzd29yZCh1c2VySWQ6IHN0cmluZywgcGFzc3dvcmQ6IHN0cmluZyk6IFByb21pc2U8c3RyaW5nPiB7XG4gICAgcmV0dXJuIHRoaXMuX2ludm9rZSgnc2V0UGFzc3dvcmQnLCB7IHVzZXJJZCwgcGFzc3dvcmQgfSwgJ3N0cmluZycpO1xuICB9XG5cbiAgLyoqXG4gICAqIFJlc2V0cyB0aGUgc3BlY2lmaWVkIHVzZXLigJlzIHBhc3N3b3JkXG4gICAqL1xuICByZXNldFBhc3N3b3JkKHVzZXJJZDogc3RyaW5nKTogUHJvbWlzZTxSZXNldFBhc3N3b3JkUmVzdWx0PiB7XG4gICAgcmV0dXJuIHRoaXMuX2ludm9rZShcbiAgICAgICdyZXNldFBhc3N3b3JkJyxcbiAgICAgIHsgdXNlcklkIH0sXG4gICAgICBBcGlTY2hlbWFzLlJlc2V0UGFzc3dvcmRSZXN1bHQsXG4gICAgKTtcbiAgfVxuXG4gIC8qKlxuICAgKiBBZGRzIG9uZSBvciBtb3JlIG5ldyByZWNvcmRzIHRvIHlvdXIgb3JnYW5pemF0aW9u4oCZcyBkYXRhXG4gICAqL1xuICBjcmVhdGUoc09iamVjdDogUmVjb3JkW10pOiBQcm9taXNlPFNhdmVSZXN1bHRbXT47XG4gIGNyZWF0ZShzT2JqZWN0OiBSZWNvcmQpOiBQcm9taXNlPFNhdmVSZXN1bHQ+O1xuICBjcmVhdGUoc09iamVjdHM6IFJlY29yZCB8IFJlY29yZFtdKTogUHJvbWlzZTxTYXZlUmVzdWx0IHwgU2F2ZVJlc3VsdFtdPjtcbiAgY3JlYXRlKHNPYmplY3RzOiBSZWNvcmQgfCBSZWNvcmRbXSkge1xuICAgIGNvbnN0IHNjaGVtYSA9IEFycmF5LmlzQXJyYXkoc09iamVjdHMpXG4gICAgICA/IFtBcGlTY2hlbWFzLlNhdmVSZXN1bHRdXG4gICAgICA6IEFwaVNjaGVtYXMuU2F2ZVJlc3VsdDtcbiAgICBjb25zdCBhcmdzID0ge1xuICAgICAgJ0B4bWxucyc6ICd1cm46cGFydG5lci5zb2FwLnNmb3JjZS5jb20nLFxuICAgICAgJ0B4bWxuczpuczEnOiAnc29iamVjdC5wYXJ0bmVyLnNvYXAuc2ZvcmNlLmNvbScsXG4gICAgICAnbnMxOnNPYmplY3RzJzogdG9Tb2FwUmVjb3JkKHNPYmplY3RzKSxcbiAgICB9O1xuICAgIHJldHVybiB0aGlzLl9pbnZva2UoJ2NyZWF0ZScsIGFyZ3MsIHNjaGVtYSk7XG4gIH1cblxuICAvKipcbiAgICogVXBkYXRlcyBvbmUgb3IgbW9yZSBleGlzdGluZyByZWNvcmRzIGluIHlvdXIgb3JnYW5pemF0aW9u4oCZcyBkYXRhLlxuICAgKi9cbiAgdXBkYXRlKHNPYmplY3Q6IFJlY29yZFtdKTogUHJvbWlzZTxTYXZlUmVzdWx0W10+O1xuICB1cGRhdGUoc09iamVjdDogUmVjb3JkKTogUHJvbWlzZTxTYXZlUmVzdWx0PjtcbiAgdXBkYXRlKHNPYmplY3RzOiBSZWNvcmQgfCBSZWNvcmRbXSk6IFByb21pc2U8U2F2ZVJlc3VsdCB8IFNhdmVSZXN1bHRbXT47XG4gIHVwZGF0ZShzT2JqZWN0czogUmVjb3JkIHwgUmVjb3JkW10pIHtcbiAgICBjb25zdCBzY2hlbWEgPSBBcnJheS5pc0FycmF5KHNPYmplY3RzKVxuICAgICAgPyBbQXBpU2NoZW1hcy5TYXZlUmVzdWx0XVxuICAgICAgOiBBcGlTY2hlbWFzLlNhdmVSZXN1bHQ7XG4gICAgY29uc3QgYXJncyA9IHtcbiAgICAgICdAeG1sbnMnOiAndXJuOnBhcnRuZXIuc29hcC5zZm9yY2UuY29tJyxcbiAgICAgICdAeG1sbnM6bnMxJzogJ3NvYmplY3QucGFydG5lci5zb2FwLnNmb3JjZS5jb20nLFxuICAgICAgJ25zMTpzT2JqZWN0cyc6IHRvU29hcFJlY29yZChzT2JqZWN0cyksXG4gICAgfTtcbiAgICByZXR1cm4gdGhpcy5faW52b2tlKCd1cGRhdGUnLCBhcmdzLCBzY2hlbWEpO1xuICB9XG5cbiAgLyoqXG4gICAqIENyZWF0ZXMgbmV3IHJlY29yZHMgYW5kIHVwZGF0ZXMgZXhpc3RpbmcgcmVjb3JkcyBpbiB5b3VyIG9yZ2FuaXphdGlvbuKAmXMgZGF0YS5cbiAgICovXG4gIHVwc2VydChcbiAgICBleHRlcm5hbElkRmllbGROYW1lOiBzdHJpbmcsXG4gICAgc09iamVjdHM6IFJlY29yZFtdLFxuICApOiBQcm9taXNlPFVwc2VydFJlc3VsdFtdPjtcbiAgdXBzZXJ0KGV4dGVybmFsSWRGaWVsZE5hbWU6IHN0cmluZywgc09iamVjdDogUmVjb3JkKTogUHJvbWlzZTxVcHNlcnRSZXN1bHQ+O1xuICB1cHNlcnQoXG4gICAgZXh0ZXJuYWxJZEZpZWxkTmFtZTogc3RyaW5nLFxuICAgIHNPYmplY3RzOiBSZWNvcmQgfCBSZWNvcmRbXSxcbiAgKTogUHJvbWlzZTxVcHNlcnRSZXN1bHQgfCBVcHNlcnRSZXN1bHRbXT47XG4gIHVwc2VydChleHRlcm5hbElkRmllbGROYW1lOiBzdHJpbmcsIHNPYmplY3RzOiBSZWNvcmQgfCBSZWNvcmRbXSkge1xuICAgIGNvbnN0IHNjaGVtYSA9IEFycmF5LmlzQXJyYXkoc09iamVjdHMpXG4gICAgICA/IFtBcGlTY2hlbWFzLlVwc2VydFJlc3VsdF1cbiAgICAgIDogQXBpU2NoZW1hcy5VcHNlcnRSZXN1bHQ7XG4gICAgY29uc3QgYXJncyA9IHtcbiAgICAgICdAeG1sbnMnOiAndXJuOnBhcnRuZXIuc29hcC5zZm9yY2UuY29tJyxcbiAgICAgICdAeG1sbnM6bnMxJzogJ3NvYmplY3QucGFydG5lci5zb2FwLnNmb3JjZS5jb20nLFxuICAgICAgJ25zMTpleHRlcm5hbElERmllbGROYW1lJzogZXh0ZXJuYWxJZEZpZWxkTmFtZSxcbiAgICAgICduczE6c09iamVjdHMnOiB0b1NvYXBSZWNvcmQoc09iamVjdHMpLFxuICAgIH07XG4gICAgcmV0dXJuIHRoaXMuX2ludm9rZSgndXBzZXJ0JywgYXJncywgc2NoZW1hKTtcbiAgfVxuXG4gIC8qKlxuICAgKiBEZWxldGVzIG9uZSBvciBtb3JlIHJlY29yZHMgZnJvbSB5b3VyIG9yZ2FuaXphdGlvbuKAmXMgZGF0YVxuICAgKi9cbiAgZGVsZXRlKGlkczogc3RyaW5nIHwgc3RyaW5nW10pOiBQcm9taXNlPERlbGV0ZVJlc3VsdFtdPjtcbiAgZGVsZXRlKGlkOiBzdHJpbmcpOiBQcm9taXNlPERlbGV0ZVJlc3VsdD47XG4gIGRlbGV0ZShpZHM6IHN0cmluZyB8IHN0cmluZ1tdKTogUHJvbWlzZTxEZWxldGVSZXN1bHQgfCBEZWxldGVSZXN1bHRbXT47XG4gIGRlbGV0ZShpZHM6IHN0cmluZyB8IHN0cmluZ1tdKSB7XG4gICAgY29uc3Qgc2NoZW1hID0gQXJyYXkuaXNBcnJheShpZHMpXG4gICAgICA/IFtBcGlTY2hlbWFzLkRlbGV0ZVJlc3VsdF1cbiAgICAgIDogQXBpU2NoZW1hcy5EZWxldGVSZXN1bHQ7XG4gICAgY29uc3QgYXJncyA9IHtcbiAgICAgICdAeG1sbnMnOiAndXJuOnBhcnRuZXIuc29hcC5zZm9yY2UuY29tJyxcbiAgICAgICdAeG1sbnM6bnMxJzogJ3NvYmplY3QucGFydG5lci5zb2FwLnNmb3JjZS5jb20nLFxuICAgICAgJ25zMTppZHMnOiBpZHMsXG4gICAgfTtcbiAgICByZXR1cm4gdGhpcy5faW52b2tlKCdkZWxldGUnLCBhcmdzLCBzY2hlbWEpO1xuICB9XG59XG5cbi8qLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0qL1xuLypcbiAqIFJlZ2lzdGVyIGhvb2sgaW4gY29ubmVjdGlvbiBpbnN0YW50aWF0aW9uIGZvciBkeW5hbWljYWxseSBhZGRpbmcgdGhpcyBBUEkgbW9kdWxlIGZlYXR1cmVzXG4gKi9cbnJlZ2lzdGVyTW9kdWxlKCdzb2FwJywgKGNvbm4pID0+IG5ldyBTb2FwQXBpKGNvbm4pKTtcblxuZXhwb3J0IGRlZmF1bHQgU29hcEFwaTtcbiJdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7OztBQUFBO0FBQ0E7QUFDQTtBQUNBO0FBQ0EsU0FBU0EsY0FBYyxRQUFRLFlBQVk7QUFFM0MsT0FBT0MsSUFBSSxNQUFNLFNBQVM7QUFFMUIsU0FDRUMsVUFBVSxRQWFMLGVBQWU7O0FBRXRCO0FBQ0E7QUFDQTtBQUNBLFNBQVNDLFlBQVlBLENBQUNDLE9BQTBCLEVBQXFCO0VBQUEsSUFBQUMsUUFBQTtFQUNuRSxPQUFPQyxvQkFBQSxDQUFBRCxRQUFBLEdBQUNFLGNBQUEsQ0FBY0gsT0FBTyxDQUFDLEdBQUdBLE9BQU8sR0FBRyxDQUFDQSxPQUFPLENBQUMsRUFBQUksSUFBQSxDQUFBSCxRQUFBLEVBQU0sVUFBQ0ksTUFBTSxFQUFLO0lBQUEsSUFBQUMsU0FBQTtJQUNwRSxJQUFRQyxJQUFJLEdB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