import _Reflect$construct from "@babel/runtime-corejs3/core-js-stable/reflect/construct";
import _Object$keys from "@babel/runtime-corejs3/core-js-stable/object/keys";
import _Object$getOwnPropertySymbols from "@babel/runtime-corejs3/core-js-stable/object/get-own-property-symbols";
import _filterInstanceProperty from "@babel/runtime-corejs3/core-js-stable/instance/filter";
import _Object$getOwnPropertyDescriptor from "@babel/runtime-corejs3/core-js-stable/object/get-own-property-descriptor";
import _Object$getOwnPropertyDescriptors from "@babel/runtime-corejs3/core-js-stable/object/get-own-property-descriptors";
import _Object$defineProperties from "@babel/runtime-corejs3/core-js-stable/object/define-properties";
import _Object$defineProperty from "@babel/runtime-corejs3/core-js-stable/object/define-property";
import _Array$from from "@babel/runtime-corejs3/core-js-stable/array/from";
import _Symbol from "@babel/runtime-corejs3/core-js-stable/symbol";
import _getIteratorMethod from "@babel/runtime-corejs3/core-js/get-iterator-method";
import _Symbol$toPrimitive from "@babel/runtime-corejs3/core-js-stable/symbol/to-primitive";
import _typeof from "@babel/runtime-corejs3/helpers/typeof";
import _toConsumableArray from "@babel/runtime-corejs3/helpers/toConsumableArray";
import _objectWithoutProperties from "@babel/runtime-corejs3/helpers/objectWithoutProperties";
import _classCallCheck from "@babel/runtime-corejs3/helpers/classCallCheck";
import _createClass from "@babel/runtime-corejs3/helpers/createClass";
import _possibleConstructorReturn from "@babel/runtime-corejs3/helpers/possibleConstructorReturn";
import _getPrototypeOf from "@babel/runtime-corejs3/helpers/getPrototypeOf";
import _inherits from "@babel/runtime-corejs3/helpers/inherits";
import _defineProperty from "@babel/runtime-corejs3/helpers/defineProperty";
import _asyncToGenerator from "@babel/runtime-corejs3/helpers/asyncToGenerator";
import _slicedToArray from "@babel/runtime-corejs3/helpers/slicedToArray";
var _excluded = ["Id", "type", "attributes"],
  _excluded2 = ["Id", "type", "attributes"],
  _excluded3 = ["Id", "type", "attributes"],
  _excluded4 = ["Id", "type", "attributes"];
function _toPropertyKey(t) { var i = _toPrimitive(t, "string"); return "symbol" == _typeof(i) ? i : i + ""; }
function _toPrimitive(t, r) { if ("object" != _typeof(t) || !t) return t; var e = t[_Symbol$toPrimitive]; if (void 0 !== e) { var i = e.call(t, r || "default"); if ("object" != _typeof(i)) return i; throw new TypeError("@@toPrimitive must return a primitive value."); } return ("string" === r ? String : Number)(t); }
function _createForOfIteratorHelper(o, allowArrayLike) { var it = typeof _Symbol !== "undefined" && _getIteratorMethod(o) || o["@@iterator"]; if (!it) { if (_Array$isArray(o) || (it = _unsupportedIterableToArray(o)) || allowArrayLike && o && typeof o.length === "number") { if (it) o = it; var i = 0; var F = function F() {}; return { s: F, n: function n() { if (i >= o.length) return { done: true }; return { done: false, value: o[i++] }; }, e: function e(_e) { throw _e; }, f: F }; } throw new TypeError("Invalid attempt to iterate non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); } var normalCompletion = true, didErr = false, err; return { s: function s() { it = it.call(o); }, n: function n() { var step = it.next(); normalCompletion = step.done; return step; }, e: function e(_e2) { didErr = true; err = _e2; }, f: function f() { try { if (!normalCompletion && it.return != null) it.return(); } finally { if (didErr) throw err; } } }; }
function _unsupportedIterableToArray(o, minLen) { var _context55; if (!o) return; if (typeof o === "string") return _arrayLikeToArray(o, minLen); var n = _sliceInstanceProperty(_context55 = Object.prototype.toString.call(o)).call(_context55, 8, -1); if (n === "Object" && o.constructor) n = o.constructor.name; if (n === "Map" || n === "Set") return _Array$from(o); if (n === "Arguments" || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n)) return _arrayLikeToArray(o, minLen); }
function _arrayLikeToArray(arr, len) { if (len == null || len > arr.length) len = arr.length; for (var i = 0, arr2 = new Array(len); i < len; i++) arr2[i] = arr[i]; return arr2; }
function ownKeys(e, r) { var t = _Object$keys(e); if (_Object$getOwnPropertySymbols) { var o = _Object$getOwnPropertySymbols(e); r && (o = _filterInstanceProperty(o).call(o, function (r) { return _Object$getOwnPropertyDescriptor(e, r).enumerable; })), t.push.apply(t, o); } return t; }
function _objectSpread(e) { for (var r = 1; r < arguments.length; r++) { var _context53, _context54; var t = null != arguments[r] ? arguments[r] : {}; r % 2 ? _forEachInstanceProperty(_context53 = ownKeys(Object(t), !0)).call(_context53, function (r) { _defineProperty(e, r, t[r]); }) : _Object$getOwnPropertyDescriptors ? _Object$defineProperties(e, _Object$getOwnPropertyDescriptors(t)) : _forEachInstanceProperty(_context54 = ownKeys(Object(t))).call(_context54, function (r) { _Object$defineProperty(e, r, _Object$getOwnPropertyDescriptor(t, r)); }); } return e; }
function _callSuper(t, o, e) { return o = _getPrototypeOf(o), _possibleConstructorReturn(t, _isNativeReflectConstruct() ? _Reflect$construct(o, e || [], _getPrototypeOf(t).constructor) : o.apply(t, e)); }
function _isNativeReflectConstruct() { try { var t = !Boolean.prototype.valueOf.call(_Reflect$construct(Boolean, [], function () {})); } catch (t) {} return (_isNativeReflectConstruct = function _isNativeReflectConstruct() { return !!t; })(); }
import _regeneratorRuntime from "@babel/runtime-corejs3/regenerator";
import _startsWithInstanceProperty from "@babel/runtime-corejs3/core-js-stable/instance/starts-with";
import _sliceInstanceProperty from "@babel/runtime-corejs3/core-js-stable/instance/slice";
import _concatInstanceProperty from "@babel/runtime-corejs3/core-js-stable/instance/concat";
import _Promise from "@babel/runtime-corejs3/core-js-stable/promise";
import _parseInt from "@babel/runtime-corejs3/core-js-stable/parse-int";
import _JSON$stringify from "@babel/runtime-corejs3/core-js-stable/json/stringify";
import _Array$isArray from "@babel/runtime-corejs3/core-js-stable/array/is-array";
import _mapInstanceProperty from "@babel/runtime-corejs3/core-js-stable/instance/map";
import _forEachInstanceProperty from "@babel/runtime-corejs3/core-js-stable/instance/for-each";
import _Object$entries from "@babel/runtime-corejs3/core-js-stable/object/entries";
import _includesInstanceProperty from "@babel/runtime-corejs3/core-js-stable/instance/includes";
import "core-js/modules/es.error.cause.js";
import "core-js/modules/es.array.join.js";
import "core-js/modules/es.function.name.js";
import "core-js/modules/es.array.push.js";
import "core-js/modules/es.number.constructor.js";
import "core-js/modules/es.object.keys.js";
import "core-js/modules/es.object.to-string.js";
import "core-js/modules/es.regexp.exec.js";
import "core-js/modules/es.regexp.test.js";
import "core-js/modules/es.regexp.to-string.js";
import "core-js/modules/es.string.match.js";
import "core-js/modules/es.string.replace.js";
/**
 *
 */
import { EventEmitter } from 'events';
import jsforce from './jsforce';
import Transport, { CanvasTransport, XdProxyTransport, HttpProxyTransport } from './transport';
import { getLogger } from './util/logger';
import OAuth2 from './oauth2';
import Cache from './cache';
import HttpApi from './http-api';
import SessionRefreshDelegate from './session-refresh-delegate';
import Query from './query';
import SObject from './sobject';
import QuickAction from './quick-action';
import Process from './process';
import { formatDate } from './util/formatter';
import FormData from 'form-data';

/**
 * type definitions
 */

/**
 *
 */
var defaultConnectionConfig = {
  loginUrl: 'https://login.salesforce.com',
  instanceUrl: '',
  version: '50.0',
  logLevel: 'NONE',
  maxRequest: 10
};

/**
 *
 */
function esc(str) {
  return String(str || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

/**
 *
 */
function parseSignedRequest(sr) {
  if (typeof sr === 'string') {
    if (_startsWithInstanceProperty(sr).call(sr, '{')) {
      // might be JSON
      return JSON.parse(sr);
    } // might be original base64-encoded signed request
    var msg = sr.split('.').pop(); // retrieve latter part
    if (!msg) {
      throw new Error('Invalid signed request');
    }
    var json = Buffer.from(msg, 'base64').toString('utf-8');
    return JSON.parse(json);
  }
  return sr;
}

/** @private **/
function parseIdUrl(url) {
  var _context;
  var _url$split$slice = _sliceInstanceProperty(_context = url.split('/')).call(_context, -2),
    _url$split$slice2 = _slicedToArray(_url$split$slice, 2),
    organizationId = _url$split$slice2[0],
    id = _url$split$slice2[1];
  return {
    id: id,
    organizationId: organizationId,
    url: url
  };
}

/**
 * Session Refresh delegate function for OAuth2 authz code flow
 * @private
 */
function oauthRefreshFn(_x, _x2) {
  return _oauthRefreshFn.apply(this, arguments);
}
/**
 * Session Refresh delegate function for username/password login
 * @private
 */
function _oauthRefreshFn() {
  _oauthRefreshFn = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee35(conn, callback) {
    var res, userInfo;
    return _regeneratorRuntime.wrap(function _callee35$(_context52) {
      while (1) switch (_context52.prev = _context52.next) {
        case 0:
          _context52.prev = 0;
          if (conn.refreshToken) {
            _context52.next = 3;
            break;
          }
          throw new Error('No refresh token found in the connection');
        case 3:
          _context52.next = 5;
          return conn.oauth2.refreshToken(conn.refreshToken);
        case 5:
          res = _context52.sent;
          userInfo = parseIdUrl(res.id);
          conn._establish({
            instanceUrl: res.instance_url,
            accessToken: res.access_token,
            userInfo: userInfo
          });
          callback(undefined, res.access_token, res);
          _context52.next = 18;
          break;
        case 11:
          _context52.prev = 11;
          _context52.t0 = _context52["catch"](0);
          if (!(_context52.t0 instanceof Error)) {
            _context52.next = 17;
            break;
          }
          callback(_context52.t0);
          _context52.next = 18;
          break;
        case 17:
          throw _context52.t0;
        case 18:
        case "end":
          return _context52.stop();
      }
    }, _callee35, null, [[0, 11]]);
  }));
  return _oauthRefreshFn.apply(this, arguments);
}
function createUsernamePasswordRefreshFn(username, password) {
  return /*#__PURE__*/function () {
    var _ref = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee(conn, callback) {
      return _regeneratorRuntime.wrap(function _callee$(_context2) {
        while (1) switch (_context2.prev = _context2.next) {
          case 0:
            _context2.prev = 0;
            _context2.next = 3;
            return conn.login(username, password);
          case 3:
            if (conn.accessToken) {
              _context2.next = 5;
              break;
            }
            throw new Error('Access token not found after login');
          case 5:
            callback(null, conn.accessToken);
            _context2.next = 15;
            break;
          case 8:
            _context2.prev = 8;
            _context2.t0 = _context2["catch"](0);
            if (!(_context2.t0 instanceof Error)) {
              _context2.next = 14;
              break;
            }
            callback(_context2.t0);
            _context2.next = 15;
            break;
          case 14:
            throw _context2.t0;
          case 15:
          case "end":
            return _context2.stop();
        }
      }, _callee, null, [[0, 8]]);
    }));
    return function (_x3, _x4) {
      return _ref.apply(this, arguments);
    };
  }();
}

/**
 * @private
 */
function toSaveResult(err) {
  return {
    success: false,
    errors: [err]
  };
}

/**
 *
 */
function raiseNoModuleError(name) {
  var _context3;
  throw new Error(_concatInstanceProperty(_context3 = "API module '".concat(name, "' is not loaded, load 'jsforce/api/")).call(_context3, name, "' explicitly"));
}

/*
 * Constant of maximum records num in DML operation (update/delete)
 */
var MAX_DML_COUNT = 200;

/**
 *
 */
export var Connection = /*#__PURE__*/function (_EventEmitter) {
  /**
   *
   */
  function Connection() {
    var _this;
    var config = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : {};
    _classCallCheck(this, Connection);
    _this = _callSuper(this, Connection);
    _defineProperty(_this, "limitInfo", {});
    _defineProperty(_this, "sobjects", {});
    /**
     * Synonym of Connection#create()
     */
    _defineProperty(_this, "insert", _this.create);
    /**
     * Synonym of Connection#destroy()
     */
    _defineProperty(_this, "delete", _this.destroy);
    /**
     * Synonym of Connection#destroy()
     */
    _defineProperty(_this, "del", _this.destroy);
    /**
     * Module which manages process rules and approval processes
     */
    _defineProperty(_this, "process", new Process(_this));
    var loginUrl = config.loginUrl,
      instanceUrl = config.instanceUrl,
      version = config.version,
      oauth2 = config.oauth2,
      maxRequest = config.maxRequest,
      logLevel = config.logLevel,
      proxyUrl = config.proxyUrl,
      httpProxy = config.httpProxy;
    _this.loginUrl = loginUrl || defaultConnectionConfig.loginUrl;
    _this.instanceUrl = instanceUrl || defaultConnectionConfig.instanceUrl;
    if (_this.isLightningInstance()) {
      throw new Error('lightning URLs are not valid as instance URLs');
    }
    _this.version = version || defaultConnectionConfig.version;
    _this.oauth2 = oauth2 instanceof OAuth2 ? oauth2 : new OAuth2(_objectSpread({
      loginUrl: _this.loginUrl,
      proxyUrl: proxyUrl,
      httpProxy: httpProxy
    }, oauth2));
    var refreshFn = config.refreshFn;
    if (!refreshFn && _this.oauth2.clientId) {
      refreshFn = oauthRefreshFn;
    }
    if (refreshFn) {
      _this._refreshDelegate = new SessionRefreshDelegate(_this, refreshFn);
    }
    _this._maxRequest = maxRequest || defaultConnectionConfig.maxRequest;
    _this._logger = logLevel ? Connection._logger.createInstance(logLevel) : Connection._logger;
    _this._logLevel = logLevel;
    _this._transport = proxyUrl ? new XdProxyTransport(proxyUrl) : httpProxy ? new HttpProxyTransport(httpProxy) : new Transport();
    _this._callOptions = config.callOptions;
    _this.cache = new Cache();
    var describeCacheKey = function describeCacheKey(type) {
      return type ? "describe.".concat(type) : 'describe';
    };
    var describe = Connection.prototype.describe;
    _this.describe = _this.cache.createCachedFunction(describe, _this, {
      key: describeCacheKey,
      strategy: 'NOCACHE'
    });
    _this.describe$ = _this.cache.createCachedFunction(describe, _this, {
      key: describeCacheKey,
      strategy: 'HIT'
    });
    _this.describe$$ = _this.cache.createCachedFunction(describe, _this, {
      key: describeCacheKey,
      strategy: 'IMMEDIATE'
    });
    _this.describeSObject = _this.describe;
    _this.describeSObject$ = _this.describe$;
    _this.describeSObject$$ = _this.describe$$;
    var describeGlobal = Connection.prototype.describeGlobal;
    _this.describeGlobal = _this.cache.createCachedFunction(describeGlobal, _this, {
      key: 'describeGlobal',
      strategy: 'NOCACHE'
    });
    _this.describeGlobal$ = _this.cache.createCachedFunction(describeGlobal, _this, {
      key: 'describeGlobal',
      strategy: 'HIT'
    });
    _this.describeGlobal$$ = _this.cache.createCachedFunction(describeGlobal, _this, {
      key: 'describeGlobal',
      strategy: 'IMMEDIATE'
    });
    var accessToken = config.accessToken,
      refreshToken = config.refreshToken,
      sessionId = config.sessionId,
      serverUrl = config.serverUrl,
      signedRequest = config.signedRequest;
    _this._establish({
      accessToken: accessToken,
      refreshToken: refreshToken,
      instanceUrl: instanceUrl,
      sessionId: sessionId,
      serverUrl: serverUrl,
      signedRequest: signedRequest
    });
    jsforce.emit('connection:new', _this);
    return _this;
  }

  /* @private */
  _inherits(Connection, _EventEmitter);
  return _createClass(Connection, [{
    key: "analytics",
    get:
    // describe: (name: string) => Promise<DescribeSObjectResult>;

    // describeGlobal: () => Promise<DescribeGlobalResult>;

    // API libs are not instantiated here so that core module to remain without dependencies to them
    // It is responsible for developers to import api libs explicitly if they are using 'jsforce/core' instead of 'jsforce'.
    function get() {
      return raiseNoModuleError('analytics');
    }
  }, {
    key: "apex",
    get: function get() {
      return raiseNoModuleError('apex');
    }
  }, {
    key: "bulk",
    get: function get() {
      return raiseNoModuleError('bulk');
    }
  }, {
    key: "bulk2",
    get: function get() {
      return raiseNoModuleError('bulk2');
    }
  }, {
    key: "chatter",
    get: function get() {
      return raiseNoModuleError('chatter');
    }
  }, {
    key: "metadata",
    get: function get() {
      return raiseNoModuleError('metadata');
    }
  }, {
    key: "soap",
    get: function get() {
      return raiseNoModuleError('soap');
    }
  }, {
    key: "streaming",
    get: function get() {
      return raiseNoModuleError('streaming');
    }
  }, {
    key: "tooling",
    get: function get() {
      return raiseNoModuleError('tooling');
    }
  }, {
    key: "_establish",
    value: function _establish(options) {
      var _context4;
      var accessToken = options.accessToken,
        refreshToken = options.refreshToken,
        instanceUrl = options.instanceUrl,
        sessionId = options.sessionId,
        serverUrl = options.serverUrl,
        signedRequest = options.signedRequest,
        userInfo = options.userInfo;
      this.instanceUrl = serverUrl ? _sliceInstanceProperty(_context4 = serverUrl.split('/')).call(_context4, 0, 3).join('/') : instanceUrl || this.instanceUrl;
      this.accessToken = sessionId || accessToken || this.accessToken;
      this.refreshToken = refreshToken || this.refreshToken;
      if (this.refreshToken && !this._refreshDelegate) {
        throw new Error('Refresh token is specified without oauth2 client information or refresh function');
      }
      var signedRequestObject = signedRequest && parseSignedRequest(signedRequest);
      if (signedRequestObject) {
        this.accessToken = signedRequestObject.client.oauthToken;
        if (CanvasTransport.supported) {
          this._transport = new CanvasTransport(signedRequestObject);
        }
      }
      this.userInfo = userInfo || this.userInfo;
      this._sessionType = sessionId ? 'soap' : 'oauth2';
      this._resetInstance();
    }

    /* @priveate */
  }, {
    key: "_clearSession",
    value: function _clearSession() {
      this.accessToken = null;
      this.refreshToken = null;
      this.instanceUrl = defaultConnectionConfig.instanceUrl;
      this.userInfo = null;
      this._sessionType = null;
    }

    /* @priveate */
  }, {
    key: "_resetInstance",
    value: function _resetInstance() {
      var _this2 = this;
      this.limitInfo = {};
      this.sobjects = {};
      // TODO impl cache
      this.cache.clear();
      this.cache.get('describeGlobal').removeAllListeners('value');
      this.cache.get('describeGlobal').on('value', function (_ref2) {
        var result = _ref2.result;
        if (result) {
          var _iterator = _createForOfIteratorHelper(result.sobjects),
            _step;
          try {
            for (_iterator.s(); !(_step = _iterator.n()).done;) {
              var so = _step.value;
              _this2.sobject(so.name);
            }
          } catch (err) {
            _iterator.e(err);
          } finally {
            _iterator.f();
          }
        }
      });
      /*
      if (this.tooling) {
        this.tooling._resetInstance();
      }
      */
    }

    /**
     * Authorize the connection using OAuth2 flow.
     * Typically, just pass the code returned from authorization server in the first argument to complete authorization.
     * If you want to authorize with grant types other than `authorization_code`, you can also pass params object with the grant type.
     *
     * @returns {Promise<UserInfo>} An object that contains the user ID, org ID and identity URL.
     *
     */
  }, {
    key: "authorize",
    value: (function () {
      var _authorize = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee2(codeOrParams) {
        var _context5;
        var params,
          res,
          userInfo,
          _args2 = arguments;
        return _regeneratorRuntime.wrap(function _callee2$(_context6) {
          while (1) switch (_context6.prev = _context6.next) {
            case 0:
              params = _args2.length > 1 && _args2[1] !== undefined ? _args2[1] : {};
              _context6.next = 3;
              return this.oauth2.requestToken(codeOrParams, params);
            case 3:
              res = _context6.sent;
              userInfo = parseIdUrl(res.id);
              this._establish({
                instanceUrl: res.instance_url,
                accessToken: res.access_token,
                refreshToken: res.refresh_token,
                userInfo: userInfo
              });
              this._logger.debug(_concatInstanceProperty(_context5 = "<login> completed. user id = ".concat(userInfo.id, ", org id = ")).call(_context5, userInfo.organizationId));
              return _context6.abrupt("return", userInfo);
            case 8:
            case "end":
              return _context6.stop();
          }
        }, _callee2, this);
      }));
      function authorize(_x5) {
        return _authorize.apply(this, arguments);
      }
      return authorize;
    }()
    /**
     *
     */
    )
  }, {
    key: "login",
    value: (function () {
      var _login = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee3(username, password) {
        var _this$oauth;
        return _regeneratorRuntime.wrap(function _callee3$(_context7) {
          while (1) switch (_context7.prev = _context7.next) {
            case 0:
              this._refreshDelegate = new SessionRefreshDelegate(this, createUsernamePasswordRefreshFn(username, password));
              if (!((_this$oauth = this.oauth2) !== null && _this$oauth !== void 0 && _this$oauth.clientId && this.oauth2.clientSecret)) {
                _context7.next = 3;
                break;
              }
              return _context7.abrupt("return", this.loginByOAuth2(username, password));
            case 3:
              return _context7.abrupt("return", this.loginBySoap(username, password));
            case 4:
            case "end":
              return _context7.stop();
          }
        }, _callee3, this);
      }));
      function login(_x6, _x7) {
        return _login.apply(this, arguments);
      }
      return login;
    }()
    /**
     * Login by OAuth2 username & password flow
     */
    )
  }, {
    key: "loginByOAuth2",
    value: (function () {
      var _loginByOAuth = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee4(username, password) {
        var _context8;
        var res, userInfo;
        return _regeneratorRuntime.wrap(function _callee4$(_context9) {
          while (1) switch (_context9.prev = _context9.next) {
            case 0:
              _context9.next = 2;
              return this.oauth2.authenticate(username, password);
            case 2:
              res = _context9.sent;
              userInfo = parseIdUrl(res.id);
              this._establish({
                instanceUrl: res.instance_url,
                accessToken: res.access_token,
                userInfo: userInfo
              });
              this._logger.info(_concatInstanceProperty(_context8 = "<login> completed. user id = ".concat(userInfo.id, ", org id = ")).call(_context8, userInfo.organizationId));
              return _context9.abrupt("return", userInfo);
            case 7:
            case "end":
              return _context9.stop();
          }
        }, _callee4, this);
      }));
      function loginByOAuth2(_x8, _x9) {
        return _loginByOAuth.apply(this, arguments);
      }
      return loginByOAuth2;
    }()
    /**
     *
     */
    )
  }, {
    key: "loginBySoap",
    value: (function () {
      var _loginBySoap = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee5(username, password) {
        var _context10, _context11;
        var body, soapLoginEndpoint, response, m, faultstring, serverUrl, sessionId, userId, organizationId, idUrl, userInfo;
        return _regeneratorRuntime.wrap(function _callee5$(_context12) {
          while (1) switch (_context12.prev = _context12.next) {
            case 0:
              if (!(!username || !password)) {
                _context12.next = 2;
                break;
              }
              return _context12.abrupt("return", _Promise.reject(new Error('no username password given')));
            case 2:
              body = ['<se:Envelope xmlns:se="http://schemas.xmlsoap.org/soap/envelope/">', '<se:Header/>', '<se:Body>', '<login xmlns="urn:partner.soap.sforce.com">', "<username>".concat(esc(username), "</username>"), "<password>".concat(esc(password), "</password>"), '</login>', '</se:Body>', '</se:Envelope>'].join('');
              soapLoginEndpoint = [this.loginUrl, 'services/Soap/u', this.version].join('/');
              _context12.next = 6;
              return this._transport.httpRequest({
                method: 'POST',
                url: soapLoginEndpoint,
                body: body,
                headers: {
                  'Content-Type': 'text/xml',
                  SOAPAction: '""'
                }
              });
            case 6:
              response = _context12.sent;
              if (!(response.statusCode >= 400)) {
                _context12.next = 11;
                break;
              }
              m = response.body.match(/<faultstring>([^<]+)<\/faultstring>/);
              faultstring = m && m[1];
              throw new Error(faultstring || response.body);
            case 11:
              if (!response.body.match(/<passwordExpired>true<\/passwordExpired>/g)) {
                _context12.next = 13;
                break;
              }
              throw new Error('Unable to login because the used password has expired.');
            case 13:
              this._logger.debug("SOAP response = ".concat(response.body));
              m = response.body.match(/<serverUrl>([^<]+)<\/serverUrl>/);
              serverUrl = m && m[1];
              m = response.body.match(/<sessionId>([^<]+)<\/sessionId>/);
              sessionId = m && m[1];
              m = response.body.match(/<userId>([^<]+)<\/userId>/);
              userId = m && m[1];
              m = response.body.match(/<organizationId>([^<]+)<\/organizationId>/);
              organizationId = m && m[1];
              if (!(!serverUrl || !sessionId || !userId || !organizationId)) {
                _context12.next = 24;
                break;
              }
              throw new Error('could not extract session information from login response');
            case 24:
              idUrl = [this.loginUrl, 'id', organizationId, userId].join('/');
              userInfo = {
                id: userId,
                organizationId: organizationId,
                url: idUrl
              };
              this._establish({
                serverUrl: _sliceInstanceProperty(_context10 = serverUrl.split('/')).call(_context10, 0, 3).join('/'),
                sessionId: sessionId,
                userInfo: userInfo
              });
              this._logger.info(_concatInstanceProperty(_context11 = "<login> completed. user id = ".concat(userId, ", org id = ")).call(_context11, organizationId));
              return _context12.abrupt("return", userInfo);
            case 29:
            case "end":
              return _context12.stop();
          }
        }, _callee5, this);
      }));
      function loginBySoap(_x10, _x11) {
        return _loginBySoap.apply(this, arguments);
      }
      return loginBySoap;
    }()
    /**
     * Logout the current session
     */
    )
  }, {
    key: "logout",
    value: (function () {
      var _logout = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee6(revoke) {
        return _regeneratorRuntime.wrap(function _callee6$(_context13) {
          while (1) switch (_context13.prev = _context13.next) {
            case 0:
              this._refreshDelegate = undefined;
              if (!(this._sessionType === 'oauth2')) {
                _context13.next = 3;
                break;
              }
              return _context13.abrupt("return", this.logoutByOAuth2(revoke));
            case 3:
              return _context13.abrupt("return", this.logoutBySoap(revoke));
            case 4:
            case "end":
              return _context13.stop();
          }
        }, _callee6, this);
      }));
      function logout(_x12) {
        return _logout.apply(this, arguments);
      }
      return logout;
    }()
    /**
     * Logout the current session by revoking access token via OAuth2 session revoke
     */
    )
  }, {
    key: "logoutByOAuth2",
    value: (function () {
      var _logoutByOAuth = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee7(revoke) {
        var token;
        return _regeneratorRuntime.wrap(function _callee7$(_context14) {
          while (1) switch (_context14.prev = _context14.next) {
            case 0:
              token = revoke ? this.refreshToken : this.accessToken;
              if (!token) {
                _context14.next = 4;
                break;
              }
              _context14.next = 4;
              return this.oauth2.revokeToken(token);
            case 4:
              // Destroy the session bound to this connection
              this._clearSession();
              this._resetInstance();
            case 6:
            case "end":
              return _context14.stop();
          }
        }, _callee7, this);
      }));
      function logoutByOAuth2(_x13) {
        return _logoutByOAuth.apply(this, arguments);
      }
      return logoutByOAuth2;
    }()
    /**
     * Logout the session by using SOAP web service API
     */
    )
  }, {
    key: "logoutBySoap",
    value: (function () {
      var _logoutBySoap = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee8(revoke) {
        var _context15;
        var body, response, m, faultstring;
        return _regeneratorRuntime.wrap(function _callee8$(_context16) {
          while (1) switch (_context16.prev = _context16.next) {
            case 0:
              body = ['<se:Envelope xmlns:se="http://schemas.xmlsoap.org/soap/envelope/">', '<se:Header>', '<SessionHeader xmlns="urn:partner.soap.sforce.com">', "<sessionId>".concat(esc(revoke ? this.refreshToken : this.accessToken), "</sessionId>"), '</SessionHeader>', '</se:Header>', '<se:Body>', '<logout xmlns="urn:partner.soap.sforce.com"/>', '</se:Body>', '</se:Envelope>'].join('');
              _context16.next = 3;
              return this._transport.httpRequest({
                method: 'POST',
                url: [this.instanceUrl, 'services/Soap/u', this.version].join('/'),
                body: body,
                headers: {
                  'Content-Type': 'text/xml',
                  SOAPAction: '""'
                }
              });
            case 3:
              response = _context16.sent;
              this._logger.debug(_concatInstanceProperty(_context15 = "SOAP statusCode = ".concat(response.statusCode, ", response = ")).call(_context15, response.body));
              if (!(response.statusCode >= 400)) {
                _context16.next = 9;
                break;
              }
              m = response.body.match(/<faultstring>([^<]+)<\/faultstring>/);
              faultstring = m && m[1];
              throw new Error(faultstring || response.body);
            case 9:
              // Destroy the session bound to this connection
              this._clearSession();
              this._resetInstance();
            case 11:
            case "end":
              return _context16.stop();
          }
        }, _callee8, this);
      }));
      function logoutBySoap(_x14) {
        return _logoutBySoap.apply(this, arguments);
      }
      return logoutBySoap;
    }()
    /**
     * Send REST API request with given HTTP request info, with connected session information.
     *
     * Endpoint URL can be absolute URL ('https://na1.salesforce.com/services/data/v32.0/sobjects/Account/describe')
     * , relative path from root ('/services/data/v32.0/sobjects/Account/describe')
     * , or relative path from version root ('/sobjects/Account/describe').
     */
    )
  }, {
    key: "request",
    value: function request(_request) {
      var _this3 = this;
      var options = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : {};
      // if request is simple string, regard it as url in GET method
      var request_ = typeof _request === 'string' ? {
        method: 'GET',
        url: _request
      } : _request;
      // if url is given in relative path, prepend base url or instance url before.
      request_ = _objectSpread(_objectSpread({}, request_), {}, {
        url: this._normalizeUrl(request_.url)
      });
      var httpApi = new HttpApi(this, options);
      // log api usage and its quota
      httpApi.on('response', function (response) {
        if (response.headers && response.headers['sforce-limit-info']) {
          var apiUsage = response.headers['sforce-limit-info'].match(/api-usage=(\d+)\/(\d+)/);
          if (apiUsage) {
            _this3.limitInfo = {
              apiUsage: {
                used: _parseInt(apiUsage[1], 10),
                limit: _parseInt(apiUsage[2], 10)
              }
            };
          }
        }
      });
      return httpApi.request(request_);
    }

    /**
     * Send HTTP GET request
     *
     * Endpoint URL can be absolute URL ('https://na1.salesforce.com/services/data/v32.0/sobjects/Account/describe')
     * , relative path from root ('/services/data/v32.0/sobjects/Account/describe')
     * , or relative path from version root ('/sobjects/Account/describe').
     */
  }, {
    key: "requestGet",
    value: function requestGet(url, options) {
      var request = {
        method: 'GET',
        url: url
      };
      return this.request(request, options);
    }

    /**
     * Send HTTP POST request with JSON body, with connected session information
     *
     * Endpoint URL can be absolute URL ('https://na1.salesforce.com/services/data/v32.0/sobjects/Account/describe')
     * , relative path from root ('/services/data/v32.0/sobjects/Account/describe')
     * , or relative path from version root ('/sobjects/Account/describe').
     */
  }, {
    key: "requestPost",
    value: function requestPost(url, body, options) {
      var request = {
        method: 'POST',
        url: url,
        body: _JSON$stringify(body),
        headers: {
          'content-type': 'application/json'
        }
      };
      return this.request(request, options);
    }

    /**
     * Send HTTP PUT request with JSON body, with connected session information
     *
     * Endpoint URL can be absolute URL ('https://na1.salesforce.com/services/data/v32.0/sobjects/Account/describe')
     * , relative path from root ('/services/data/v32.0/sobjects/Account/describe')
     * , or relative path from version root ('/sobjects/Account/describe').
     */
  }, {
    key: "requestPut",
    value: function requestPut(url, body, options) {
      var request = {
        method: 'PUT',
        url: url,
        body: _JSON$stringify(body),
        headers: {
          'content-type': 'application/json'
        }
      };
      return this.request(request, options);
    }

    /**
     * Send HTTP PATCH request with JSON body
     *
     * Endpoint URL can be absolute URL ('https://na1.salesforce.com/services/data/v32.0/sobjects/Account/describe')
     * , relative path from root ('/services/data/v32.0/sobjects/Account/describe')
     * , or relative path from version root ('/sobjects/Account/describe').
     */
  }, {
    key: "requestPatch",
    value: function requestPatch(url, body, options) {
      var request = {
        method: 'PATCH',
        url: url,
        body: _JSON$stringify(body),
        headers: {
          'content-type': 'application/json'
        }
      };
      return this.request(request, options);
    }

    /**
     * Send HTTP DELETE request
     *
     * Endpoint URL can be absolute URL ('https://na1.salesforce.com/services/data/v32.0/sobjects/Account/describe')
     * , relative path from root ('/services/data/v32.0/sobjects/Account/describe')
     * , or relative path from version root ('/sobjects/Account/describe').
     */
  }, {
    key: "requestDelete",
    value: function requestDelete(url, options) {
      var request = {
        method: 'DELETE',
        url: url
      };
      return this.request(request, options);
    }

    /** @private **/
  }, {
    key: "_baseUrl",
    value: function _baseUrl() {
      return [this.instanceUrl, 'services/data', "v".concat(this.version)].join('/');
    }

    /**
     * Convert path to absolute url
     * @private
     */
  }, {
    key: "_normalizeUrl",
    value: function _normalizeUrl(url) {
      if (_startsWithInstanceProperty(url).call(url, '/')) {
        if (_startsWithInstanceProperty(url).call(url, this.instanceUrl + '/services/')) {
          return url;
        }
        if (_startsWithInstanceProperty(url).call(url, '/services/')) {
          return this.instanceUrl + url;
        }
        return this._baseUrl() + url;
      }
      return url;
    }

    /**
     *
     */
  }, {
    key: "query",
    value: function query(soql, options) {
      return new Query(this, soql, options);
    }

    /**
     * Execute search by SOSL
     *
     * @param {String} sosl - SOSL string
     * @param {Callback.<Array.<RecordResult>>} [callback] - Callback function
     * @returns {Promise.<Array.<RecordResult>>}
     */
  }, {
    key: "search",
    value: function search(sosl) {
      var url = this._baseUrl() + '/search?q=' + encodeURIComponent(sosl);
      return this.request(url);
    }

    /**
     *
     */
  }, {
    key: "queryMore",
    value: function queryMore(locator, options) {
      return new Query(this, {
        locator: locator
      }, options);
    }

    /* */
  }, {
    key: "_ensureVersion",
    value: function _ensureVersion(majorVersion) {
      var versions = this.version.split('.');
      return _parseInt(versions[0], 10) >= majorVersion;
    }

    /* */
  }, {
    key: "_supports",
    value: function _supports(feature) {
      switch (feature) {
        case 'sobject-collection':
          // sobject collection is available only in API ver 42.0+
          return this._ensureVersion(42);
        default:
          return false;
      }
    }

    /**
     * Retrieve specified records
     */
  }, {
    key: "retrieve",
    value: function () {
      var _retrieve = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee9(type, ids) {
        var options,
          _args9 = arguments;
        return _regeneratorRuntime.wrap(function _callee9$(_context17) {
          while (1) switch (_context17.prev = _context17.next) {
            case 0:
              options = _args9.length > 2 && _args9[2] !== undefined ? _args9[2] : {};
              return _context17.abrupt("return", _Array$isArray(ids) ?
              // check the version whether SObject collection API is supported (42.0)
              this._ensureVersion(42) ? this._retrieveMany(type, ids, options) : this._retrieveParallel(type, ids, options) : this._retrieveSingle(type, ids, options));
            case 2:
            case "end":
              return _context17.stop();
          }
        }, _callee9, this);
      }));
      function retrieve(_x15, _x16) {
        return _retrieve.apply(this, arguments);
      }
      return retrieve;
    }() /** @private */
  }, {
    key: "_retrieveSingle",
    value: (function () {
      var _retrieveSingle2 = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee10(type, id, options) {
        var url, fields, headers;
        return _regeneratorRuntime.wrap(function _callee10$(_context18) {
          while (1) switch (_context18.prev = _context18.next) {
            case 0:
              if (id) {
                _context18.next = 2;
                break;
              }
              throw new Error('Invalid record ID. Specify valid record ID value');
            case 2:
              url = [this._baseUrl(), 'sobjects', type, id].join('/');
              fields = options.fields, headers = options.headers;
              if (fields) {
                url += "?fields=".concat(fields.join(','));
              }
              return _context18.abrupt("return", this.request({
                method: 'GET',
                url: url,
                headers: headers
              }));
            case 6:
            case "end":
              return _context18.stop();
          }
        }, _callee10, this);
      }));
      function _retrieveSingle(_x17, _x18, _x19) {
        return _retrieveSingle2.apply(this, arguments);
      }
      return _retrieveSingle;
    }() /** @private */)
  }, {
    key: "_retrieveParallel",
    value: (function () {
      var _retrieveParallel2 = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee11(type, ids, options) {
        var _this4 = this;
        return _regeneratorRuntime.wrap(function _callee11$(_context19) {
          while (1) switch (_context19.prev = _context19.next) {
            case 0:
              if (!(ids.length > this._maxRequest)) {
                _context19.next = 2;
                break;
              }
              throw new Error('Exceeded max limit of concurrent call');
            case 2:
              return _context19.abrupt("return", _Promise.all(_mapInstanceProperty(ids).call(ids, function (id) {
                return _this4._retrieveSingle(type, id, options).catch(function (err) {
                  if (options.allOrNone || err.errorCode !== 'NOT_FOUND') {
                    throw err;
                  }
                  return null;
                });
              })));
            case 3:
            case "end":
              return _context19.stop();
          }
        }, _callee11, this);
      }));
      function _retrieveParallel(_x20, _x21, _x22) {
        return _retrieveParallel2.apply(this, arguments);
      }
      return _retrieveParallel;
    }() /** @private */)
  }, {
    key: "_retrieveMany",
    value: (function () {
      var _retrieveMany2 = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee12(type, ids, options) {
        var _context20;
        var url, fields;
        return _regeneratorRuntime.wrap(function _callee12$(_context21) {
          while (1) switch (_context21.prev = _context21.next) {
            case 0:
              if (!(ids.length === 0)) {
                _context21.next = 2;
                break;
              }
              return _context21.abrupt("return", []);
            case 2:
              url = [this._baseUrl(), 'composite', 'sobjects', type].join('/');
              _context21.t0 = options.fields;
              if (_context21.t0) {
                _context21.next = 10;
                break;
              }
              _context21.t1 = _mapInstanceProperty;
              _context21.next = 8;
              return this.describe$(type);
            case 8:
              _context21.t2 = _context20 = _context21.sent.fields;
              _context21.t0 = (0, _context21.t1)(_context21.t2).call(_context20, function (field) {
                return field.name;
              });
            case 10:
              fields = _context21.t0;
              return _context21.abrupt("return", this.request({
                method: 'POST',
                url: url,
                body: _JSON$stringify({
                  ids: ids,
                  fields: fields
                }),
                headers: _objectSpread(_objectSpread({}, options.headers || {}), {}, {
                  'content-type': 'application/json'
                })
              }));
            case 12:
            case "end":
              return _context21.stop();
          }
        }, _callee12, this);
      }));
      function _retrieveMany(_x23, _x24, _x25) {
        return _retrieveMany2.apply(this, arguments);
      }
      return _retrieveMany;
    }()
    /**
     * Create records
     */
    )
  }, {
    key: "create",
    value: (
    /**
     * @param type
     * @param records
     * @param options
     */
    function () {
      var _create = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee13(type, records) {
        var options,
          ret,
          _args13 = arguments;
        return _regeneratorRuntime.wrap(function _callee13$(_context22) {
          while (1) switch (_context22.prev = _context22.next) {
            case 0:
              options = _args13.length > 2 && _args13[2] !== undefined ? _args13[2] : {};
              if (!_Array$isArray(records)) {
                _context22.next = 14;
                break;
              }
              if (!this._ensureVersion(42)) {
                _context22.next = 8;
                break;
              }
              _context22.next = 5;
              return this._createMany(type, records, options);
            case 5:
              _context22.t1 = _context22.sent;
              _context22.next = 11;
              break;
            case 8:
              _context22.next = 10;
              return this._createParallel(type, records, options);
            case 10:
              _context22.t1 = _context22.sent;
            case 11:
              _context22.t0 = _context22.t1;
              _context22.next = 17;
              break;
            case 14:
              _context22.next = 16;
              return this._createSingle(type, records, options);
            case 16:
              _context22.t0 = _context22.sent;
            case 17:
              ret = _context22.t0;
              return _context22.abrupt("return", ret);
            case 19:
            case "end":
              return _context22.stop();
          }
        }, _callee13, this);
      }));
      function create(_x26, _x27) {
        return _create.apply(this, arguments);
      }
      return create;
    }() /** @private */)
  }, {
    key: "_createSingle",
    value: (function () {
      var _createSingle2 = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee14(type, record, options) {
        var Id, rtype, attributes, rec, sobjectType, url, contentType, body, _context23, form;
        return _regeneratorRuntime.wrap(function _callee14$(_context24) {
          while (1) switch (_context24.prev = _context24.next) {
            case 0:
              Id = record.Id, rtype = record.type, attributes = record.attributes, rec = _objectWithoutProperties(record, _excluded);
              sobjectType = type || (attributes === null || attributes === void 0 ? void 0 : attributes.type) || rtype;
              if (sobjectType) {
                _context24.next = 4;
                break;
              }
              throw new Error('No SObject Type defined in record');
            case 4:
              url = [this._baseUrl(), 'sobjects', sobjectType].join('/');
              if (options !== null && options !== void 0 && options.multipartFileFields) {
                // Send the record as a multipart/form-data request. Useful for fields containing large binary blobs.
                form = new FormData(); // Extract the fields requested to be sent separately from the JSON
                _forEachInstanceProperty(_context23 = _Object$entries(options.multipartFileFields)).call(_context23, function (_ref3) {
                  var _ref4 = _slicedToArray(_ref3, 2),
                    fieldName = _ref4[0],
                    fileDetails = _ref4[1];
                  form.append(fieldName, Buffer.from(rec[fieldName], 'base64'), fileDetails);
                  delete rec[fieldName];
                });
                // Serialize the remaining fields as JSON
                form.append(type, _JSON$stringify(rec), {
                  contentType: 'application/json'
                });
                contentType = form.getHeaders()['content-type']; // This is necessary to ensure the 'boundary' is present
                body = form;
              } else {
                // Default behavior: send the request as JSON
                contentType = 'application/json';
                body = _JSON$stringify(rec);
              }
              return _context24.abrupt("return", this.request({
                method: 'POST',
                url: url,
                body: body,
                headers: _objectSpread(_objectSpread({}, options.headers || {}), {}, {
                  'content-type': contentType
                })
              }));
            case 7:
            case "end":
              return _context24.stop();
          }
        }, _callee14, this);
      }));
      function _createSingle(_x28, _x29, _x30) {
        return _createSingle2.apply(this, arguments);
      }
      return _createSingle;
    }() /** @private */)
  }, {
    key: "_createParallel",
    value: (function () {
      var _createParallel2 = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee15(type, records, options) {
        var _this5 = this;
        return _regeneratorRuntime.wrap(function _callee15$(_context25) {
          while (1) switch (_context25.prev = _context25.next) {
            case 0:
              if (!(records.length > this._maxRequest)) {
                _context25.next = 2;
                break;
              }
              throw new Error('Exceeded max limit of concurrent call');
            case 2:
              return _context25.abrupt("return", _Promise.all(_mapInstanceProperty(records).call(records, function (record) {
                return _this5._createSingle(type, record, options).catch(function (err) {
                  // be aware that allOrNone in parallel mode will not revert the other successful requests
                  // it only raises error when met at least one failed request.
                  if (options.allOrNone || !err.errorCode) {
                    throw err;
                  }
                  return toSaveResult(err);
                });
              })));
            case 3:
            case "end":
              return _context25.stop();
          }
        }, _callee15, this);
      }));
      function _createParallel(_x31, _x32, _x33) {
        return _createParallel2.apply(this, arguments);
      }
      return _createParallel;
    }() /** @private */)
  }, {
    key: "_createMany",
    value: (function () {
      var _createMany2 = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee16(type, records, options) {
        var _context26, _records, url;
        return _regeneratorRuntime.wrap(function _callee16$(_context27) {
          while (1) switch (_context27.prev = _context27.next) {
            case 0:
              if (!(records.length === 0)) {
                _context27.next = 2;
                break;
              }
              return _context27.abrupt("return", _Promise.resolve([]));
            case 2:
              if (!(records.length > MAX_DML_COUNT && options.allowRecursive)) {
                _context27.next = 16;
                break;
              }
              _context27.t0 = _concatInstanceProperty(_context26 = []);
              _context27.t1 = _context26;
              _context27.t2 = _toConsumableArray;
              _context27.next = 8;
              return this._createMany(type, _sliceInstanceProperty(records).call(records, 0, MAX_DML_COUNT), options);
            case 8:
              _context27.t3 = _context27.sent;
              _context27.t4 = (0, _context27.t2)(_context27.t3);
              _context27.t5 = _toConsumableArray;
              _context27.next = 13;
              return this._createMany(type, _sliceInstanceProperty(records).call(records, MAX_DML_COUNT), options);
            case 13:
              _context27.t6 = _context27.sent;
              _context27.t7 = (0, _context27.t5)(_context27.t6);
              return _context27.abrupt("return", _context27.t0.call.call(_context27.t0, _context27.t1, _context27.t4, _context27.t7));
            case 16:
              _records = _mapInstanceProperty(records).call(records, function (record) {
                var Id = record.Id,
                  rtype = record.type,
                  attributes = record.attributes,
                  rec = _objectWithoutProperties(record, _excluded2);
                var sobjectType = type || (attributes === null || attributes === void 0 ? void 0 : attributes.type) || rtype;
                if (!sobjectType) {
                  throw new Error('No SObject Type defined in record');
                }
                return _objectSpread({
                  attributes: {
                    type: sobjectType
                  }
                }, rec);
              });
              url = [this._baseUrl(), 'composite', 'sobjects'].join('/');
              return _context27.abrupt("return", this.request({
                method: 'POST',
                url: url,
                body: _JSON$stringify({
                  allOrNone: options.allOrNone || false,
                  records: _records
                }),
                headers: _objectSpread(_objectSpread({}, options.headers || {}), {}, {
                  'content-type': 'application/json'
                })
              }));
            case 19:
            case "end":
              return _context27.stop();
          }
        }, _callee16, this);
      }));
      function _createMany(_x34, _x35, _x36) {
        return _createMany2.apply(this, arguments);
      }
      return _createMany;
    }())
  }, {
    key: "update",
    value:
    /**
     * @param type
     * @param records
     * @param options
     */
    function update(type, records) {
      var options = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : {};
      return _Array$isArray(records) ?
      // check the version whether SObject collection API is supported (42.0)
      this._ensureVersion(42) ? this._updateMany(type, records, options) : this._updateParallel(type, records, options) : this._updateSingle(type, records, options);
    }

    /** @private */
  }, {
    key: "_updateSingle",
    value: (function () {
      var _updateSingle2 = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee17(type, record, options) {
        var id, rtype, attributes, rec, sobjectType, url;
        return _regeneratorRuntime.wrap(function _callee17$(_context28) {
          while (1) switch (_context28.prev = _context28.next) {
            case 0:
              id = record.Id, rtype = record.type, attributes = record.attributes, rec = _objectWithoutProperties(record, _excluded3);
              if (id) {
                _context28.next = 3;
                break;
              }
              throw new Error('Record id is not found in record.');
            case 3:
              sobjectType = type || (attributes === null || attributes === void 0 ? void 0 : attributes.type) || rtype;
              if (sobjectType) {
                _context28.next = 6;
                break;
              }
              throw new Error('No SObject Type defined in record');
            case 6:
              url = [this._baseUrl(), 'sobjects', sobjectType, id].join('/');
              return _context28.abrupt("return", this.request({
                method: 'PATCH',
                url: url,
                body: _JSON$stringify(rec),
                headers: _objectSpread(_objectSpread({}, options.headers || {}), {}, {
                  'content-type': 'application/json'
                })
              }, {
                noContentResponse: {
                  id: id,
                  success: true,
                  errors: []
                }
              }));
            case 8:
            case "end":
              return _context28.stop();
          }
        }, _callee17, this);
      }));
      function _updateSingle(_x37, _x38, _x39) {
        return _updateSingle2.apply(this, arguments);
      }
      return _updateSingle;
    }() /** @private */)
  }, {
    key: "_updateParallel",
    value: (function () {
      var _updateParallel2 = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee18(type, records, options) {
        var _this6 = this;
        return _regeneratorRuntime.wrap(function _callee18$(_context29) {
          while (1) switch (_context29.prev = _context29.next) {
            case 0:
              if (!(records.length > this._maxRequest)) {
                _context29.next = 2;
                break;
              }
              throw new Error('Exceeded max limit of concurrent call');
            case 2:
              return _context29.abrupt("return", _Promise.all(_mapInstanceProperty(records).call(records, function (record) {
                return _this6._updateSingle(type, record, options).catch(function (err) {
                  // be aware that allOrNone in parallel mode will not revert the other successful requests
                  // it only raises error when met at least one failed request.
                  if (options.allOrNone || !err.errorCode) {
                    throw err;
                  }
                  return toSaveResult(err);
                });
              })));
            case 3:
            case "end":
              return _context29.stop();
          }
        }, _callee18, this);
      }));
      function _updateParallel(_x40, _x41, _x42) {
        return _updateParallel2.apply(this, arguments);
      }
      return _updateParallel;
    }() /** @private */)
  }, {
    key: "_updateMany",
    value: (function () {
      var _updateMany2 = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee19(type, records, options) {
        var _context30, _records, url;
        return _regeneratorRuntime.wrap(function _callee19$(_context31) {
          while (1) switch (_context31.prev = _context31.next) {
            case 0:
              if (!(records.length === 0)) {
                _context31.next = 2;
                break;
              }
              return _context31.abrupt("return", []);
            case 2:
              if (!(records.length > MAX_DML_COUNT && options.allowRecursive)) {
                _context31.next = 16;
                break;
              }
              _context31.t0 = _concatInstanceProperty(_context30 = []);
              _context31.t1 = _context30;
              _context31.t2 = _toConsumableArray;
              _context31.next = 8;
              return this._updateMany(type, _sliceInstanceProperty(records).call(records, 0, MAX_DML_COUNT), options);
            case 8:
              _context31.t3 = _context31.sent;
              _context31.t4 = (0, _context31.t2)(_context31.t3);
              _context31.t5 = _toConsumableArray;
              _context31.next = 13;
              return this._updateMany(type, _sliceInstanceProperty(records).call(records, MAX_DML_COUNT), options);
            case 13:
              _context31.t6 = _context31.sent;
              _context31.t7 = (0, _context31.t5)(_context31.t6);
              return _context31.abrupt("return", _context31.t0.call.call(_context31.t0, _context31.t1, _context31.t4, _context31.t7));
            case 16:
              _records = _mapInstanceProperty(records).call(records, function (record) {
                var id = record.Id,
                  rtype = record.type,
                  attributes = record.attributes,
                  rec = _objectWithoutProperties(record, _excluded4);
                if (!id) {
                  throw new Error('Record id is not found in record.');
                }
                var sobjectType = type || (attributes === null || attributes === void 0 ? void 0 : attributes.type) || rtype;
                if (!sobjectType) {
                  throw new Error('No SObject Type defined in record');
                }
                return _objectSpread({
                  id: id,
                  attributes: {
                    type: sobjectType
                  }
                }, rec);
              });
              url = [this._baseUrl(), 'composite', 'sobjects'].join('/');
              return _context31.abrupt("return", this.request({
                method: 'PATCH',
                url: url,
                body: _JSON$stringify({
                  allOrNone: options.allOrNone || false,
                  records: _records
                }),
                headers: _objectSpread(_objectSpread({}, options.headers || {}), {}, {
                  'content-type': 'application/json'
                })
              }));
            case 19:
            case "end":
              return _context31.stop();
          }
        }, _callee19, this);
      }));
      function _updateMany(_x43, _x44, _x45) {
        return _updateMany2.apply(this, arguments);
      }
      return _updateMany;
    }()
    /**
     * Upsert records
     */
    )
  }, {
    key: "upsert",
    value: (
    /**
     *
     * @param type
     * @param records
     * @param extIdField
     * @param options
     */
    function () {
      var _upsert = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee20(type, records, extIdField) {
        var _this7 = this;
        var options,
          isArray,
          _records,
          results,
          _args20 = arguments;
        return _regeneratorRuntime.wrap(function _callee20$(_context33) {
          while (1) switch (_context33.prev = _context33.next) {
            case 0:
              options = _args20.length > 3 && _args20[3] !== undefined ? _args20[3] : {};
              isArray = _Array$isArray(records);
              _records = _Array$isArray(records) ? records : [records];
              if (!(_records.length > this._maxRequest)) {
                _context33.next = 5;
                break;
              }
              throw new Error('Exceeded max limit of concurrent call');
            case 5:
              _context33.next = 7;
              return _Promise.all(_mapInstanceProperty(_records).call(_records, function (record) {
                var _context32;
                var extId = record[extIdField],
                  rtype = record.type,
                  attributes = record.attributes,
                  rec = _objectWithoutProperties(record, _mapInstanceProperty(_context32 = [extIdField, "type", "attributes"]).call(_context32, _toPropertyKey));
                var url = [_this7._baseUrl(), 'sobjects', type, extIdField, extId].join('/');
                return _this7.request({
                  method: 'PATCH',
                  url: url,
                  body: _JSON$stringify(rec),
                  headers: _objectSpread(_objectSpread({}, options.headers || {}), {}, {
                    'content-type': 'application/json'
                  })
                }, {
                  noContentResponse: {
                    success: true,
                    errors: []
                  }
                }).catch(function (err) {
                  // Be aware that `allOrNone` option in upsert method
                  // will not revert the other successful requests.
                  // It only raises error when met at least one failed request.
                  if (!isArray || options.allOrNone || !err.errorCode) {
                    throw err;
                  }
                  return toSaveResult(err);
                });
              }));
            case 7:
              results = _context33.sent;
              return _context33.abrupt("return", isArray ? results : results[0]);
            case 9:
            case "end":
              return _context33.stop();
          }
        }, _callee20, this);
      }));
      function upsert(_x46, _x47, _x48) {
        return _upsert.apply(this, arguments);
      }
      return upsert;
    }()
    /**
     * Delete records
     */
    )
  }, {
    key: "destroy",
    value: (
    /**
     * @param type
     * @param ids
     * @param options
     */
    function () {
      var _destroy = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee21(type, ids) {
        var options,
          _args21 = arguments;
        return _regeneratorRuntime.wrap(function _callee21$(_context34) {
          while (1) switch (_context34.prev = _context34.next) {
            case 0:
              options = _args21.length > 2 && _args21[2] !== undefined ? _args21[2] : {};
              return _context34.abrupt("return", _Array$isArray(ids) ?
              // check the version whether SObject collection API is supported (42.0)
              this._ensureVersion(42) ? this._destroyMany(type, ids, options) : this._destroyParallel(type, ids, options) : this._destroySingle(type, ids, options));
            case 2:
            case "end":
              return _context34.stop();
          }
        }, _callee21, this);
      }));
      function destroy(_x49, _x50) {
        return _destroy.apply(this, arguments);
      }
      return destroy;
    }() /** @private */)
  }, {
    key: "_destroySingle",
    value: (function () {
      var _destroySingle2 = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee22(type, id, options) {
        var url;
        return _regeneratorRuntime.wrap(function _callee22$(_context35) {
          while (1) switch (_context35.prev = _context35.next) {
            case 0:
              url = [this._baseUrl(), 'sobjects', type, id].join('/');
              return _context35.abrupt("return", this.request({
                method: 'DELETE',
                url: url,
                headers: options.headers || {}
              }, {
                noContentResponse: {
                  id: id,
                  success: true,
                  errors: []
                }
              }));
            case 2:
            case "end":
              return _context35.stop();
          }
        }, _callee22, this);
      }));
      function _destroySingle(_x51, _x52, _x53) {
        return _destroySingle2.apply(this, arguments);
      }
      return _destroySingle;
    }() /** @private */)
  }, {
    key: "_destroyParallel",
    value: (function () {
      var _destroyParallel2 = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee23(type, ids, options) {
        var _this8 = this;
        return _regeneratorRuntime.wrap(function _callee23$(_context36) {
          while (1) switch (_context36.prev = _context36.next) {
            case 0:
              if (!(ids.length > this._maxRequest)) {
                _context36.next = 2;
                break;
              }
              throw new Error('Exceeded max limit of concurrent call');
            case 2:
              return _context36.abrupt("return", _Promise.all(_mapInstanceProperty(ids).call(ids, function (id) {
                return _this8._destroySingle(type, id, options).catch(function (err) {
                  // Be aware that `allOrNone` option in parallel mode
                  // will not revert the other successful requests.
                  // It only raises error when met at least one failed request.
                  if (options.allOrNone || !err.errorCode) {
                    throw err;
                  }
                  return toSaveResult(err);
                });
              })));
            case 3:
            case "end":
              return _context36.stop();
          }
        }, _callee23, this);
      }));
      function _destroyParallel(_x54, _x55, _x56) {
        return _destroyParallel2.apply(this, arguments);
      }
      return _destroyParallel;
    }() /** @private */)
  }, {
    key: "_destroyMany",
    value: (function () {
      var _destroyMany2 = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee24(type, ids, options) {
        var _context37, url;
        return _regeneratorRuntime.wrap(function _callee24$(_context38) {
          while (1) switch (_context38.prev = _context38.next) {
            case 0:
              if (!(ids.length === 0)) {
                _context38.next = 2;
                break;
              }
              return _context38.abrupt("return", []);
            case 2:
              if (!(ids.length > MAX_DML_COUNT && options.allowRecursive)) {
                _context38.next = 16;
                break;
              }
              _context38.t0 = _concatInstanceProperty(_context37 = []);
              _context38.t1 = _context37;
              _context38.t2 = _toConsumableArray;
              _context38.next = 8;
              return this._destroyMany(type, _sliceInstanceProperty(ids).call(ids, 0, MAX_DML_COUNT), options);
            case 8:
              _context38.t3 = _context38.sent;
              _context38.t4 = (0, _context38.t2)(_context38.t3);
              _context38.t5 = _toConsumableArray;
              _context38.next = 13;
              return this._destroyMany(type, _sliceInstanceProperty(ids).call(ids, MAX_DML_COUNT), options);
            case 13:
              _context38.t6 = _context38.sent;
              _context38.t7 = (0, _context38.t5)(_context38.t6);
              return _context38.abrupt("return", _context38.t0.call.call(_context38.t0, _context38.t1, _context38.t4, _context38.t7));
            case 16:
              url = [this._baseUrl(), 'composite', 'sobjects?ids='].join('/') + ids.join(',');
              if (options.allOrNone) {
                url += '&allOrNone=true';
              }
              return _context38.abrupt("return", this.request({
                method: 'DELETE',
                url: url,
                headers: options.headers || {}
              }));
            case 19:
            case "end":
              return _context38.stop();
          }
        }, _callee24, this);
      }));
      function _destroyMany(_x57, _x58, _x59) {
        return _destroyMany2.apply(this, arguments);
      }
      return _destroyMany;
    }())
  }, {
    key: "describe",
    value: (
    /**
     * Describe SObject metadata
     */
    function () {
      var _describe = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee25(type) {
        var url, body;
        return _regeneratorRuntime.wrap(function _callee25$(_context39) {
          while (1) switch (_context39.prev = _context39.next) {
            case 0:
              url = [this._baseUrl(), 'sobjects', type, 'describe'].join('/');
              _context39.next = 3;
              return this.request(url);
            case 3:
              body = _context39.sent;
              return _context39.abrupt("return", body);
            case 5:
            case "end":
              return _context39.stop();
          }
        }, _callee25, this);
      }));
      function describe(_x60) {
        return _describe.apply(this, arguments);
      }
      return describe;
    }()
    /**
     * Describe global SObjects
     */
    )
  }, {
    key: "describeGlobal",
    value: (function () {
      var _describeGlobal = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee26() {
        var url, body;
        return _regeneratorRuntime.wrap(function _callee26$(_context40) {
          while (1) switch (_context40.prev = _context40.next) {
            case 0:
              url = "".concat(this._baseUrl(), "/sobjects");
              _context40.next = 3;
              return this.request(url);
            case 3:
              body = _context40.sent;
              return _context40.abrupt("return", body);
            case 5:
            case "end":
              return _context40.stop();
          }
        }, _callee26, this);
      }));
      function describeGlobal() {
        return _describeGlobal.apply(this, arguments);
      }
      return describeGlobal;
    }()
    /**
     * Get SObject instance
     */
    )
  }, {
    key: "sobject",
    value: function sobject(type) {
      var so = this.sobjects[type] || new SObject(this, type);
      this.sobjects[type] = so;
      return so;
    }

    /**
     * Get identity information of current user
     */
  }, {
    key: "identity",
    value: (function () {
      var _identity = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee27() {
        var _this$userInfo;
        var options,
          url,
          _res,
          res,
          _args27 = arguments;
        return _regeneratorRuntime.wrap(function _callee27$(_context41) {
          while (1) switch (_context41.prev = _context41.next) {
            case 0:
              options = _args27.length > 0 && _args27[0] !== undefined ? _args27[0] : {};
              url = (_this$userInfo = this.userInfo) === null || _this$userInfo === void 0 ? void 0 : _this$userInfo.url;
              if (url) {
                _context41.next = 7;
                break;
              }
              _context41.next = 5;
              return this.request({
                method: 'GET',
                url: this._baseUrl(),
                headers: options.headers
              });
            case 5:
              _res = _context41.sent;
              url = _res.identity;
            case 7:
              url += '?format=json';
              if (this.accessToken) {
                url += "&oauth_token=".concat(encodeURIComponent(this.accessToken));
              }
              _context41.next = 11;
              return this.request({
                method: 'GET',
                url: url
              });
            case 11:
              res = _context41.sent;
              this.userInfo = {
                id: res.user_id,
                organizationId: res.organization_id,
                url: res.id
              };
              return _context41.abrupt("return", res);
            case 14:
            case "end":
              return _context41.stop();
          }
        }, _callee27, this);
      }));
      function identity() {
        return _identity.apply(this, arguments);
      }
      return identity;
    }()
    /**
     * List recently viewed records
     */
    )
  }, {
    key: "recent",
    value: (function () {
      var _recent = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee28(type, limit) {
        var url, _yield$this$request, recentItems;
        return _regeneratorRuntime.wrap(function _callee28$(_context42) {
          while (1) switch (_context42.prev = _context42.next) {
            case 0:
              /* eslint-disable no-param-reassign */
              if (typeof type === 'number') {
                limit = type;
                type = undefined;
              }
              if (!type) {
                _context42.next = 8;
                break;
              }
              url = [this._baseUrl(), 'sobjects', type].join('/');
              _context42.next = 5;
              return this.request(url);
            case 5:
              _yield$this$request = _context42.sent;
              recentItems = _yield$this$request.recentItems;
              return _context42.abrupt("return", limit ? _sliceInstanceProperty(recentItems).call(recentItems, 0, limit) : recentItems);
            case 8:
              url = "".concat(this._baseUrl(), "/recent");
              if (limit) {
                url += "?limit=".concat(limit);
              }
              return _context42.abrupt("return", this.request(url));
            case 11:
            case "end":
              return _context42.stop();
          }
        }, _callee28, this);
      }));
      function recent(_x61, _x62) {
        return _recent.apply(this, arguments);
      }
      return recent;
    }()
    /**
     * Retrieve updated records
     */
    )
  }, {
    key: "updated",
    value: (function () {
      var _updated = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee29(type, start, end) {
        var url, body;
        return _regeneratorRuntime.wrap(function _callee29$(_context43) {
          while (1) switch (_context43.prev = _context43.next) {
            case 0:
              /* eslint-disable no-param-reassign */
              url = [this._baseUrl(), 'sobjects', type, 'updated'].join('/');
              if (typeof start === 'string') {
                start = new Date(start);
              }
              start = formatDate(start);
              url += "?start=".concat(encodeURIComponent(start));
              if (typeof end === 'string') {
                end = new Date(end);
              }
              end = formatDate(end);
              url += "&end=".concat(encodeURIComponent(end));
              _context43.next = 9;
              return this.request(url);
            case 9:
              body = _context43.sent;
              return _context43.abrupt("return", body);
            case 11:
            case "end":
              return _context43.stop();
          }
        }, _callee29, this);
      }));
      function updated(_x63, _x64, _x65) {
        return _updated.apply(this, arguments);
      }
      return updated;
    }()
    /**
     * Retrieve deleted records
     */
    )
  }, {
    key: "deleted",
    value: (function () {
      var _deleted = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee30(type, start, end) {
        var url, body;
        return _regeneratorRuntime.wrap(function _callee30$(_context44) {
          while (1) switch (_context44.prev = _context44.next) {
            case 0:
              /* eslint-disable no-param-reassign */
              url = [this._baseUrl(), 'sobjects', type, 'deleted'].join('/');
              if (typeof start === 'string') {
                start = new Date(start);
              }
              start = formatDate(start);
              url += "?start=".concat(encodeURIComponent(start));
              if (typeof end === 'string') {
                end = new Date(end);
              }
              end = formatDate(end);
              url += "&end=".concat(encodeURIComponent(end));
              _context44.next = 9;
              return this.request(url);
            case 9:
              body = _context44.sent;
              return _context44.abrupt("return", body);
            case 11:
            case "end":
              return _context44.stop();
          }
        }, _callee30, this);
      }));
      function deleted(_x66, _x67, _x68) {
        return _deleted.apply(this, arguments);
      }
      return deleted;
    }()
    /**
     * Returns a list of all tabs
     */
    )
  }, {
    key: "tabs",
    value: (function () {
      var _tabs = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee31() {
        var url, body;
        return _regeneratorRuntime.wrap(function _callee31$(_context45) {
          while (1) switch (_context45.prev = _context45.next) {
            case 0:
              url = [this._baseUrl(), 'tabs'].join('/');
              _context45.next = 3;
              return this.request(url);
            case 3:
              body = _context45.sent;
              return _context45.abrupt("return", body);
            case 5:
            case "end":
              return _context45.stop();
          }
        }, _callee31, this);
      }));
      function tabs() {
        return _tabs.apply(this, arguments);
      }
      return tabs;
    }()
    /**
     * Returns current system limit in the organization
     */
    )
  }, {
    key: "limits",
    value: (function () {
      var _limits = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee32() {
        var url, body;
        return _regeneratorRuntime.wrap(function _callee32$(_context46) {
          while (1) switch (_context46.prev = _context46.next) {
            case 0:
              url = [this._baseUrl(), 'limits'].join('/');
              _context46.next = 3;
              return this.request(url);
            case 3:
              body = _context46.sent;
              return _context46.abrupt("return", body);
            case 5:
            case "end":
              return _context46.stop();
          }
        }, _callee32, this);
      }));
      function limits() {
        return _limits.apply(this, arguments);
      }
      return limits;
    }()
    /**
     * Returns a theme info
     */
    )
  }, {
    key: "theme",
    value: (function () {
      var _theme = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee33() {
        var url, body;
        return _regeneratorRuntime.wrap(function _callee33$(_context47) {
          while (1) switch (_context47.prev = _context47.next) {
            case 0:
              url = [this._baseUrl(), 'theme'].join('/');
              _context47.next = 3;
              return this.request(url);
            case 3:
              body = _context47.sent;
              return _context47.abrupt("return", body);
            case 5:
            case "end":
              return _context47.stop();
          }
        }, _callee33, this);
      }));
      function theme() {
        return _theme.apply(this, arguments);
      }
      return theme;
    }()
    /**
     * Returns all registered global quick actions
     */
    )
  }, {
    key: "quickActions",
    value: (function () {
      var _quickActions = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee34() {
        var body;
        return _regeneratorRuntime.wrap(function _callee34$(_context48) {
          while (1) switch (_context48.prev = _context48.next) {
            case 0:
              _context48.next = 2;
              return this.request('/quickActions');
            case 2:
              body = _context48.sent;
              return _context48.abrupt("return", body);
            case 4:
            case "end":
              return _context48.stop();
          }
        }, _callee34, this);
      }));
      function quickActions() {
        return _quickActions.apply(this, arguments);
      }
      return quickActions;
    }()
    /**
     * Get reference for specified global quick action
     */
    )
  }, {
    key: "quickAction",
    value: function quickAction(actionName) {
      return new QuickAction(this, "/quickActions/".concat(actionName));
    }
  }, {
    key: "isLightningInstance",
    value: function isLightningInstance() {
      var _context49, _context50, _context51;
      return _includesInstanceProperty(_context49 = this.instanceUrl).call(_context49, '.lightning.force.com') || _includesInstanceProperty(_context50 = this.instanceUrl).call(_context50, '.lightning.crmforce.mil') || _includesInstanceProperty(_context51 = this.instanceUrl).call(_context51, '.lightning.sfcrmapps.cn');
    }
  }]);
}(EventEmitter);
_defineProperty(Connection, "_logger", getLogger('connection'));
export default Connection;
//# sourceMappingURL=data:application/json;charset=utf-8;base64,eyJ2ZXJzaW9uIjozLCJuYW1lcyI6WyJFdmVudEVtaXR0ZXIiLCJqc2ZvcmNlIiwiVHJhbnNwb3J0IiwiQ2FudmFzVHJhbnNwb3J0IiwiWGRQcm94eVRyYW5zcG9ydCIsIkh0dHBQcm94eVRyYW5zcG9ydCIsImdldExvZ2dlciIsIk9BdXRoMiIsIkNhY2hlIiwiSHR0cEFwaSIsIlNlc3Npb25SZWZyZXNoRGVsZWdhdGUiLCJRdWVyeSIsIlNPYmplY3QiLCJRdWlja0FjdGlvbiIsIlByb2Nlc3MiLCJmb3JtYXREYXRlIiwiRm9ybURhdGEiLCJkZWZhdWx0Q29ubmVjdGlvbkNvbmZpZyIsImxvZ2luVXJsIiwiaW5zdGFuY2VVcmwiLCJ2ZXJzaW9uIiwibG9nTGV2ZWwiLCJtYXhSZXF1ZXN0IiwiZXNjIiwic3RyIiwiU3RyaW5nIiwicmVwbGFjZSIsInBhcnNlU2lnbmVkUmVxdWVzdCIsInNyIiwiX3N0YXJ0c1dpdGhJbnN0YW5jZVByb3BlcnR5IiwiY2FsbCIsIkpTT04iLCJwYXJzZSIsIm1zZyIsInNwbGl0IiwicG9wIiwiRXJyb3IiLCJqc29uIiwiQnVmZmVyIiwiZnJvbSIsInRvU3RyaW5nIiwicGFyc2VJZFVybCIsInVybCIsIl9jb250ZXh0IiwiX3VybCRzcGxpdCRzbGljZSIsIl9zbGljZUluc3RhbmNlUHJvcGVydHkiLCJfdXJsJHNwbGl0JHNsaWNlMiIsIl9zbGljZWRUb0FycmF5Iiwib3JnYW5pemF0aW9uSWQiLCJpZCIsIm9hdXRoUmVmcmVzaEZuIiwiX3giLCJfeDIiLCJfb2F1dGhSZWZyZXNoRm4iLCJhcHBseSIsImFyZ3VtZW50cyIsIl9hc3luY1RvR2VuZXJhdG9yIiwiX3JlZ2VuZXJhdG9yUnVudGltZSIsIm1hcmsiLCJfY2FsbGVlMzUiLCJjb25uIiwiY2FsbGJhY2siLCJyZXMiLCJ1c2VySW5mbyIsIndyYXAiLCJfY2FsbGVlMzUkIiwiX2NvbnRleHQ1MiIsInByZXYiLCJuZXh0IiwicmVmcmVzaFRva2VuIiwib2F1dGgyIiwic2VudCIsIl9lc3RhYmxpc2giLCJpbnN0YW5jZV91cmwiLCJhY2Nlc3NUb2tlbiIsImFjY2Vzc190b2tlbiIsInVuZGVmaW5lZCIsInQwIiwic3RvcCIsImNyZWF0ZVVzZXJuYW1lUGFzc3dvcmRSZWZyZXNoRm4iLCJ1c2VybmFtZSIsInBhc3N3b3JkIiwiX3JlZiIsIl9jYWxsZWUiLCJfY2FsbGVlJCIsIl9jb250ZXh0MiIsImxvZ2luIiwiX3gzIiwiX3g0IiwidG9TYXZlUmVzdWx0IiwiZXJyIiwic3VjY2VzcyIsImVycm9ycyIsInJhaXNlTm9Nb2R1bGVFcnJvciIsIm5hbWUiLCJfY29udGV4dDMiLCJfY29uY2F0SW5zdGFuY2VQcm9wZXJ0eSIsImNvbmNhdCIsIk1BWF9ETUxfQ09VTlQiLCJDb25uZWN0aW9uIiwiX0V2ZW50RW1pdHRlciIsIl90aGlzIiwiY29uZmlnIiwibGVuZ3RoIiwiX2NsYXNzQ2FsbENoZWNrIiwiX2NhbGxTdXBlciIsIl9kZWZpbmVQcm9wZXJ0eSIsImNyZWF0ZSIsImRlc3Ryb3kiLCJwcm94eVVybCIsImh0dHBQcm94eSIsImlzTGlnaHRuaW5nSW5zdGFuY2UiLCJfb2JqZWN0U3ByZWFkIiwicmVmcmVzaEZuIiwiY2xpZW50SWQiLCJfcmVmcmVzaERlbGVnYXRlIiwiX21heFJlcXVlc3QiLCJfbG9nZ2VyIiwiY3JlYXRlSW5zdGFuY2UiLCJfbG9nTGV2ZWwiLCJfdHJhbnNwb3J0IiwiX2NhbGxPcHRpb25zIiwiY2FsbE9wdGlvbnMiLCJjYWNoZSIsImRlc2NyaWJlQ2FjaGVLZXkiLCJ0eXBlIiwiZGVzY3JpYmUiLCJwcm90b3R5cGUiLCJjcmVhdGVDYWNoZWRGdW5jdGlvbiIsImtleSIsInN0cmF0ZWd5IiwiZGVzY3JpYmUkIiwiZGVzY3JpYmUkJCIsImRlc2NyaWJlU09iamVjdCIsImRlc2NyaWJlU09iamVjdCQiLCJkZXNjcmliZVNPYmplY3QkJCIsImRlc2NyaWJlR2xvYmFsIiwiZGVzY3JpYmVHbG9iYWwkIiwiZGVzY3JpYmVHbG9iYWwkJCIsInNlc3Npb25JZCIsInNlcnZlclVybCIsInNpZ25lZFJlcXVlc3QiLCJlbWl0IiwiX2luaGVyaXRzIiwiX2NyZWF0ZUNsYXNzIiwiZ2V0IiwidmFsdWUiLCJvcHRpb25zIiwiX2NvbnRleHQ0Iiwiam9pbiIsInNpZ25lZFJlcXVlc3RPYmplY3QiLCJjbGllbnQiLCJvYXV0aFRva2VuIiwic3VwcG9ydGVkIiwiX3Nlc3Npb25UeXBlIiwiX3Jlc2V0SW5zdGFuY2UiLCJfY2xlYXJTZXNzaW9uIiwiX3RoaXMyIiwibGltaXRJbmZvIiwic29iamVjdHMiLCJjbGVhciIsInJlbW92ZUFsbExpc3RlbmVycyIsIm9uIiwiX3JlZjIiLCJyZXN1bHQiLCJfaXRlcmF0b3IiLCJfY3JlYXRlRm9yT2ZJdGVyYXRvckhlbHBlciIsIl9zdGVwIiwicyIsIm4iLCJkb25lIiwic28iLCJzb2JqZWN0IiwiZSIsImYiLCJfYXV0aG9yaXplIiwiX2NhbGxlZTIiLCJjb2RlT3JQYXJhbXMiLCJfY29udGV4dDUiLCJwYXJhbXMiLCJfYXJnczIiLCJfY2FsbGVlMiQiLCJfY29udGV4dDYiLCJyZXF1ZXN0VG9rZW4iLCJyZWZyZXNoX3Rva2VuIiwiZGVidWciLCJhYnJ1cHQiLCJhdXRob3JpemUiLCJfeDUiLCJfbG9naW4iLCJfY2FsbGVlMyIsIl90aGlzJG9hdXRoIiwiX2NhbGxlZTMkIiwiX2NvbnRleHQ3IiwiY2xpZW50U2VjcmV0IiwibG9naW5CeU9BdXRoMiIsImxvZ2luQnlTb2FwIiwiX3g2IiwiX3g3IiwiX2xvZ2luQnlPQXV0aCIsIl9jYWxsZWU0IiwiX2NvbnRleHQ4IiwiX2NhbGxlZTQkIiwiX2NvbnRleHQ5IiwiYXV0aGVudGljYXRlIiwiaW5mbyIsIl94OCIsIl94OSIsIl9sb2dpbkJ5U29hcCIsIl9jYWxsZWU1IiwiX2NvbnRleHQxMCIsIl9jb250ZXh0MTEiLCJib2R5Iiwic29hcExvZ2luRW5kcG9pbnQiLCJyZXNwb25zZSIsIm0iLCJmYXVsdHN0cmluZyIsInVzZXJJZCIsImlkVXJsIiwiX2NhbGxlZTUkIiwiX2NvbnRleHQxMiIsIl9Qcm9taXNlIiwicmVqZWN0IiwiaHR0cFJlcXVlc3QiLCJtZXRob2QiLCJoZWFkZXJzIiwiU09BUEFjdGlvbiIsInN0YXR1c0NvZGUiLCJtYXRjaCIsIl94MTAiLCJfeDExIiwiX2xvZ291dCIsIl9jYWxsZWU2IiwicmV2b2tlIiwiX2NhbGxlZTYkIiwiX2NvbnRleHQxMyIsImxvZ291dEJ5T0F1dGgyIiwibG9nb3V0QnlTb2FwIiwibG9nb3V0IiwiX3gxMiIsIl9sb2dvdXRCeU9BdXRoIiwiX2NhbGxlZTciLCJ0b2tlbiIsIl9jYWxsZWU3JCIsIl9jb250ZXh0MTQiLCJyZXZva2VUb2tlbiIsIl94MTMiLCJfbG9nb3V0QnlTb2FwIiwiX2NhbGxlZTgiLCJfY29udGV4dDE1IiwiX2NhbGxlZTgkIiwiX2NvbnRleHQxNiIsIl94MTQiLCJyZXF1ZXN0IiwiX3RoaXMzIiwicmVxdWVzdF8iLCJfbm9ybWFsaXplVXJsIiwiaHR0cEFwaSIsImFwaVVzYWdlIiwidXNlZCIsIl9wYXJzZUludCIsImxpbWl0IiwicmVxdWVzdEdldCIsInJlcXVlc3RQb3N0IiwiX0pTT04kc3RyaW5naWZ5IiwicmVxdWVzdFB1dCIsInJlcXVlc3RQYXRjaCIsInJlcXVlc3REZWxldGUiLCJfYmFzZVVybCIsInF1ZXJ5Iiwic29xbCIsInNlYXJjaCIsInNvc2wiLCJlbmNvZGVVUklDb21wb25lbnQiLCJxdWVyeU1vcmUiLCJsb2NhdG9yIiwiX2Vuc3VyZVZlcnNpb24iLCJtYWpvclZlcnNpb24iLCJ2ZXJzaW9ucyIsIl9zdXBwb3J0cyIsImZlYXR1cmUiLCJfcmV0cmlldmUiLCJfY2FsbGVlOSIsImlkcyIsIl9hcmdzOSIsIl9jYWxsZWU5JCIsIl9jb250ZXh0MTciLCJfQXJyYXkkaXNBcnJheSIsIl9yZXRyaWV2ZU1hbnkiLCJfcmV0cmlldmVQYXJhbGxlbCIsIl9yZXRyaWV2ZVNpbmdsZSIsInJldHJpZXZlIiwiX3gxNSIsIl94MTYiLCJfcmV0cmlldmVTaW5nbGUyIiwiX2NhbGxlZTEwIiwiZmllbGRzIiwiX2NhbGxlZTEwJCIsIl9jb250ZXh0MTgiLCJfeDE3IiwiX3gxOCIsIl94MTkiLCJfcmV0cmlldmVQYXJhbGxlbDIiLCJfY2FsbGVlMTEiLCJfdGhpczQiLCJfY2FsbGVlMTEkIiwiX2NvbnRleHQxOSIsImFsbCIsIl9tYXBJbnN0YW5jZVByb3BlcnR5IiwiY2F0Y2giLCJhbGxPck5vbmUiLCJlcnJvckNvZGUiLCJfeDIwIiwiX3gyMSIsIl94MjIiLCJfcmV0cmlldmVNYW55MiIsIl9jYWxsZWUxMiIsIl9jb250ZXh0MjAiLCJfY2FsbGVlMTIkIiwiX2NvbnRleHQyMSIsInQxIiwidDIiLCJmaWVsZCIsIl94MjMiLCJfeDI0IiwiX3gyNSIsIl9jcmVhdGUiLCJfY2FsbGVlMTMiLCJyZWNvcmRzIiwicmV0IiwiX2FyZ3MxMyIsIl9jYWxsZWUxMyQiLCJfY29udGV4dDIyIiwiX2NyZWF0ZU1hbnkiLCJfY3JlYXRlUGFyYWxsZWwiLCJfY3JlYXRlU2luZ2xlIiwiX3gyNiIsIl94MjciLCJfY3JlYXRlU2luZ2xlMiIsIl9jYWxsZWUxNCIsInJlY29yZCIsIklkIiwicnR5cGUiLCJhdHRyaWJ1dGVzIiwicmVjIiwic29iamVjdFR5cGUiLCJjb250ZW50VHlwZSIsIl9jb250ZXh0MjMiLCJmb3JtIiwiX2NhbGxlZTE0JCIsIl9jb250ZXh0MjQiLCJfb2JqZWN0V2l0aG91dFByb3BlcnRpZXMiLCJfZXhjbHVkZWQiLCJtdWx0aXBhcnRGaWxlRmllbGRzIiwiX2ZvckVhY2hJbnN0YW5jZVByb3BlcnR5IiwiX09iamVjdCRlbnRyaWVzIiwiX3JlZjMiLCJfcmVmNCIsImZpZWxkTmFtZSIsImZpbGVEZXRhaWxzIiwiYXBwZW5kIiwiZ2V0SGVhZGVycyIsIl94MjgiLCJfeDI5IiwiX3gzMCIsIl9jcmVhdGVQYXJhbGxlbDIiLCJfY2FsbGVlMTUiLCJfdGhpczUiLCJfY2FsbGVlMTUkIiwiX2NvbnRleHQyNSIsIl94MzEiLCJfeDMyIiwiX3gzMyIsIl9jcmVhdGVNYW55MiIsIl9jYWxsZWUxNiIsIl9jb250ZXh0MjYiLCJfcmVjb3JkcyIsIl9jYWxsZWUxNiQiLCJfY29udGV4dDI3IiwicmVzb2x2ZSIsImFsbG93UmVjdXJzaXZlIiwiX3RvQ29uc3VtYWJsZUFycmF5IiwidDMiLCJ0NCIsInQ1IiwidDYiLCJ0NyIsIl9leGNsdWRlZDIiLCJfeDM0IiwiX3gzNSIsIl94MzYiLCJ1cGRhdGUiLCJfdXBkYXRlTWFueSIsIl91cGRhdGVQYXJhbGxlbCIsIl91cGRhdGVTaW5nbGUiLCJfdXBkYXRlU2luZ2xlMiIsIl9jYWxsZWUxNyIsIl9jYWxsZWUxNyQiLCJfY29udGV4dDI4IiwiX2V4Y2x1ZGVkMyIsIm5vQ29udGVudFJlc3BvbnNlIiwiX3gzNyIsIl94MzgiLCJfeDM5IiwiX3VwZGF0ZVBhcmFsbGVsMiIsIl9jYWxsZWUxOCIsIl90aGlzNiIsIl9jYWxsZWUxOCQiLCJfY29udGV4dDI5IiwiX3g0MCIsIl94NDEiLCJfeDQyIiwiX3VwZGF0ZU1hbnkyIiwiX2NhbGxlZTE5IiwiX2NvbnRleHQzMCIsIl9jYWxsZWUxOSQiLCJfY29udGV4dDMxIiwiX2V4Y2x1ZGVkNCIsIl94NDMiLCJfeDQ0IiwiX3g0NSIsIl91cHNlcnQiLCJfY2FsbGVlMjAiLCJleHRJZEZpZWxkIiwiX3RoaXM3IiwiaXNBcnJheSIsInJlc3VsdHMiLCJfYXJnczIwIiwiX2NhbGxlZTIwJCIsIl9jb250ZXh0MzMiLCJfY29udGV4dDMyIiwiZXh0SWQiLCJfdG9Qcm9wZXJ0eUtleSIsInVwc2VydCIsIl94NDYiLCJfeDQ3IiwiX3g0OCIsIl9kZXN0cm95IiwiX2NhbGxlZTIxIiwiX2FyZ3MyMSIsIl9jYWxsZWUyMSQiLCJfY29udGV4dDM0IiwiX2Rlc3Ryb3lNYW55IiwiX2Rlc3Ryb3lQYXJhbGxlbCIsIl9kZXN0cm95U2luZ2xlIiwiX3g0OSIsIl94NTAiLCJfZGVzdHJveVNpbmdsZTIiLCJfY2FsbGVlMjIiLCJfY2FsbGVlMjIkIiwiX2NvbnRleHQzNSIsIl94NTEiLCJfeDUyIiwiX3g1MyIsIl9kZXN0cm95UGFyYWxsZWwyIiwiX2NhbGxlZTIzIiwiX3RoaXM4IiwiX2NhbGxlZTIzJCIsIl9jb250ZXh0MzYiLCJfeDU0IiwiX3g1NSIsIl94NTYiLCJfZGVzdHJveU1hbnkyIiwiX2NhbGxlZTI0IiwiX2NvbnRleHQzNyIsIl9jYWxsZWUyNCQiLCJfY29udGV4dDM4IiwiX3g1NyIsIl94NTgiLCJfeDU5IiwiX2Rlc2NyaWJlIiwiX2NhbGxlZTI1IiwiX2NhbGxlZTI1JCIsIl9jb250ZXh0MzkiLCJfeDYwIiwiX2Rlc2NyaWJlR2xvYmFsIiwiX2NhbGxlZTI2IiwiX2NhbGxlZTI2JCIsIl9jb250ZXh0NDAiLCJfaWRlbnRpdHkiLCJfY2FsbGVlMjciLCJfdGhpcyR1c2VySW5mbyIsIl9yZXMiLCJfYXJnczI3IiwiX2NhbGxlZTI3JCIsIl9jb250ZXh0NDEiLCJpZGVudGl0eSIsInVzZXJfaWQiLCJvcmdhbml6YXRpb25faWQiLCJfcmVjZW50IiwiX2NhbGxlZTI4IiwiX3lpZWxkJHRoaXMkcmVxdWVzdCIsInJlY2VudEl0ZW1zIiwiX2NhbGxlZTI4JCIsIl9jb250ZXh0NDIiLCJyZWNlbnQiLCJfeDYxIiwiX3g2MiIsIl91cGRhdGVkIiwiX2NhbGxlZTI5Iiwic3RhcnQiLCJlbmQiLCJfY2FsbGVlMjkkIiwiX2NvbnRleHQ0MyIsIkRhdGUiLCJ1cGRhdGVkIiwiX3g2MyIsIl94NjQiLCJfeDY1IiwiX2RlbGV0ZWQiLCJfY2FsbGVlMzAiLCJfY2FsbGVlMzAkIiwiX2NvbnRleHQ0NCIsImRlbGV0ZWQiLCJfeDY2IiwiX3g2NyIsIl94NjgiLCJfdGFicyIsIl9jYWxsZWUzMSIsIl9jYWxsZWUzMSQiLCJfY29udGV4dDQ1IiwidGFicyIsIl9saW1pdHMiLCJfY2FsbGVlMzIiLCJfY2FsbGVlMzIkIiwiX2NvbnRleHQ0NiIsImxpbWl0cyIsIl90aGVtZSIsIl9jYWxsZWUzMyIsIl9jYWxsZWUzMyQiLCJfY29udGV4dDQ3IiwidGhlbWUiLCJfcXVpY2tBY3Rpb25zIiwiX2NhbGxlZTM0IiwiX2NhbGxlZTM0JCIsIl9jb250ZXh0NDgiLCJxdWlja0FjdGlvbnMiLCJxdWlja0FjdGlvbiIsImFjdGlvbk5hbWUiLCJfY29udGV4dDQ5IiwiX2NvbnRleHQ1MCIsIl9jb250ZXh0NTEiLCJfaW5jbHVkZXNJbnN0YW5jZVByb3BlcnR5Il0sInNvdXJjZXMiOlsiLi4vc3JjL2Nvbm5lY3Rpb24udHMiXSwic291cmNlc0NvbnRlbnQiOlsiLyoqXG4gKlxuICovXG5pbXBvcnQgeyBFdmVudEVtaXR0ZXIgfSBmcm9tICdldmVudHMnO1xuaW1wb3J0IGpzZm9yY2UgZnJvbSAnLi9qc2ZvcmNlJztcbmltcG9ydCB7XG4gIEh0dHBSZXF1ZXN0LFxuICBIdHRwUmVzcG9uc2UsXG4gIENhbGxiYWNrLFxuICBSZWNvcmQsXG4gIFNhdmVSZXN1bHQsXG4gIFVwc2VydFJlc3VsdCxcbiAgRGVzY3JpYmVHbG9iYWxSZXN1bHQsXG4gIERlc2NyaWJlU09iamVjdFJlc3VsdCxcbiAgRGVzY3JpYmVUYWIsXG4gIERlc2NyaWJlVGhlbWUsXG4gIERlc2NyaWJlUXVpY2tBY3Rpb25SZXN1bHQsXG4gIFVwZGF0ZWRSZXN1bHQsXG4gIERlbGV0ZWRSZXN1bHQsXG4gIFNlYXJjaFJlc3VsdCxcbiAgT3JnYW5pemF0aW9uTGltaXRzSW5mbyxcbiAgT3B0aW9uYWwsXG4gIFNpZ25lZFJlcXVlc3RPYmplY3QsXG4gIFNhdmVFcnJvcixcbiAgRG1sT3B0aW9ucyxcbiAgUmV0cmlldmVPcHRpb25zLFxuICBTY2hlbWEsXG4gIFNPYmplY3ROYW1lcyxcbiAgU09iamVjdElucHV0UmVjb3JkLFxuICBTT2JqZWN0VXBkYXRlUmVjb3JkLFxuICBTT2JqZWN0RmllbGROYW1lcyxcbiAgVXNlckluZm8sXG4gIElkZW50aXR5SW5mbyxcbiAgTGltaXRJbmZvLFxufSBmcm9tICcuL3R5cGVzJztcbmltcG9ydCB7IFN0cmVhbVByb21pc2UgfSBmcm9tICcuL3V0aWwvcHJvbWlzZSc7XG5pbXBvcnQgVHJhbnNwb3J0LCB7XG4gIENhbnZhc1RyYW5zcG9ydCxcbiAgWGRQcm94eVRyYW5zcG9ydCxcbiAgSHR0cFByb3h5VHJhbnNwb3J0LFxufSBmcm9tICcuL3RyYW5zcG9ydCc7XG5pbXBvcnQgeyBMb2dnZXIsIGdldExvZ2dlciB9IGZyb20gJy4vdXRpbC9sb2dnZXInO1xuaW1wb3J0IHsgTG9nTGV2ZWxDb25maWcgfSBmcm9tICcuL3V0aWwvbG9nZ2VyJztcbmltcG9ydCBPQXV0aDIsIHsgVG9rZW5SZXNwb25zZSB9IGZyb20gJy4vb2F1dGgyJztcbmltcG9ydCB7IE9BdXRoMkNvbmZpZyB9IGZyb20gJy4vb2F1dGgyJztcbmltcG9ydCBDYWNoZSwgeyBDYWNoZWRGdW5jdGlvbiB9IGZyb20gJy4vY2FjaGUnO1xuaW1wb3J0IEh0dHBBcGkgZnJvbSAnLi9odHRwLWFwaSc7XG5pbXBvcnQgU2Vzc2lvblJlZnJlc2hEZWxlZ2F0ZSwge1xuICBTZXNzaW9uUmVmcmVzaEZ1bmMsXG59IGZyb20gJy4vc2Vzc2lvbi1yZWZyZXNoLWRlbGVnYXRlJztcbmltcG9ydCBRdWVyeSBmcm9tICcuL3F1ZXJ5JztcbmltcG9ydCB7IFF1ZXJ5T3B0aW9ucyB9IGZyb20gJy4vcXVlcnknO1xuaW1wb3J0IFNPYmplY3QgZnJvbSAnLi9zb2JqZWN0JztcbmltcG9ydCBRdWlja0FjdGlvbiBmcm9tICcuL3F1aWNrLWFjdGlvbic7XG5pbXBvcnQgUHJvY2VzcyBmcm9tICcuL3Byb2Nlc3MnO1xuaW1wb3J0IHsgZm9ybWF0RGF0ZSB9IGZyb20gJy4vdXRpbC9mb3JtYXR0ZXInO1xuaW1wb3J0IEFuYWx5dGljcyBmcm9tICcuL2FwaS9hbmFseXRpY3MnO1xuaW1wb3J0IEFwZXggZnJvbSAnLi9hcGkvYXBleCc7XG5pbXBvcnQgeyBCdWxrIH0gZnJvbSAnLi9hcGkvYnVsayc7XG5pbXBvcnQgeyBCdWxrVjIgfSBmcm9tICcuL2FwaS9idWxrMic7XG5pbXBvcnQgQ2hhdHRlciBmcm9tICcuL2FwaS9jaGF0dGVyJztcbmltcG9ydCBNZXRhZGF0YSBmcm9tICcuL2FwaS9tZXRhZGF0YSc7XG5pbXBvcnQgU29hcEFwaSBmcm9tICcuL2FwaS9zb2FwJztcbmltcG9ydCBTdHJlYW1pbmcgZnJvbSAnLi9hcGkvc3RyZWFtaW5nJztcbmltcG9ydCBUb29saW5nIGZyb20gJy4vYXBpL3Rvb2xpbmcnO1xuaW1wb3J0IEZvcm1EYXRhIGZyb20gJ2Zvcm0tZGF0YSc7XG5cbi8qKlxuICogdHlwZSBkZWZpbml0aW9uc1xuICovXG5leHBvcnQgdHlwZSBDb25uZWN0aW9uQ29uZmlnPFMgZXh0ZW5kcyBTY2hlbWEgPSBTY2hlbWE+ID0ge1xuICB2ZXJzaW9uPzogc3RyaW5nO1xuICBsb2dpblVybD86IHN0cmluZztcbiAgYWNjZXNzVG9rZW4/OiBzdHJpbmc7XG4gIHJlZnJlc2hUb2tlbj86IHN0cmluZztcbiAgaW5zdGFuY2VVcmw/OiBzdHJpbmc7XG4gIHNlc3Npb25JZD86IHN0cmluZztcbiAgc2VydmVyVXJsPzogc3RyaW5nO1xuICBzaWduZWRSZXF1ZXN0Pzogc3RyaW5nO1xuICBvYXV0aDI/OiBPQXV0aDIgfCBPQXV0aDJDb25maWc7XG4gIG1heFJlcXVlc3Q/OiBudW1iZXI7XG4gIHByb3h5VXJsPzogc3RyaW5nO1xuICBodHRwUHJveHk/OiBzdHJpbmc7XG4gIGxvZ0xldmVsPzogTG9nTGV2ZWxDb25maWc7XG4gIGNhbGxPcHRpb25zPzogeyBbbmFtZTogc3RyaW5nXTogc3RyaW5nIH07XG4gIHJlZnJlc2hGbj86IFNlc3Npb25SZWZyZXNoRnVuYzxTPjtcbn07XG5cbmV4cG9ydCB0eXBlIENvbm5lY3Rpb25Fc3RhYmxpc2hPcHRpb25zID0ge1xuICBhY2Nlc3NUb2tlbj86IE9wdGlvbmFsPHN0cmluZz47XG4gIHJlZnJlc2hUb2tlbj86IE9wdGlvbmFsPHN0cmluZz47XG4gIGluc3RhbmNlVXJsPzogT3B0aW9uYWw8c3RyaW5nPjtcbiAgc2Vzc2lvbklkPzogT3B0aW9uYWw8c3RyaW5nPjtcbiAgc2VydmVyVXJsPzogT3B0aW9uYWw8c3RyaW5nPjtcbiAgc2lnbmVkUmVxdWVzdD86IE9wdGlvbmFsPHN0cmluZyB8IFNpZ25lZFJlcXVlc3RPYmplY3Q+O1xuICB1c2VySW5mbz86IE9wdGlvbmFsPFVzZXJJbmZvPjtcbn07XG5cbi8qKlxuICpcbiAqL1xuY29uc3QgZGVmYXVsdENvbm5lY3Rpb25Db25maWc6IHtcbiAgbG9naW5Vcmw6IHN0cmluZztcbiAgaW5zdGFuY2VVcmw6IHN0cmluZztcbiAgdmVyc2lvbjogc3RyaW5nO1xuICBsb2dMZXZlbDogTG9nTGV2ZWxDb25maWc7XG4gIG1heFJlcXVlc3Q6IG51bWJlcjtcbn0gPSB7XG4gIGxvZ2luVXJsOiAnaHR0cHM6Ly9sb2dpbi5zYWxlc2ZvcmNlLmNvbScsXG4gIGluc3RhbmNlVXJsOiAnJyxcbiAgdmVyc2lvbjogJzUwLjAnLFxuICBsb2dMZXZlbDogJ05PTkUnLFxuICBtYXhSZXF1ZXN0OiAxMCxcbn07XG5cbi8qKlxuICpcbiAqL1xuZnVuY3Rpb24gZXNjKHN0cjogT3B0aW9uYWw8c3RyaW5nPik6IHN0cmluZyB7XG4gIHJldHVybiBTdHJpbmcoc3RyIHx8ICcnKVxuICAgIC5yZXBsYWNlKC8mL2csICcmYW1wOycpXG4gICAgLnJlcGxhY2UoLzwvZywgJyZsdDsnKVxuICAgIC5yZXBsYWNlKC8+L2csICcmZ3Q7JylcbiAgICAucmVwbGFjZSgvXCIvZywgJyZxdW90OycpO1xufVxuXG4vKipcbiAqXG4gKi9cbmZ1bmN0aW9uIHBhcnNlU2lnbmVkUmVxdWVzdChzcjogc3RyaW5nIHwgT2JqZWN0KTogU2lnbmVkUmVxdWVzdE9iamVjdCB7XG4gIGlmICh0eXBlb2Ygc3IgPT09ICdzdHJpbmcnKSB7XG4gICAgaWYgKHNyLnN0YXJ0c1dpdGgoJ3snKSkge1xuICAgICAgLy8gbWlnaHQgYmUgSlNPTlxuICAgICAgcmV0dXJuIEpTT04ucGFyc2Uoc3IpO1xuICAgIH0gLy8gbWlnaHQgYmUgb3JpZ2luYWwgYmFzZTY0LWVuY29kZWQgc2lnbmVkIHJlcXVlc3RcbiAgICBjb25zdCBtc2cgPSBzci5zcGxpdCgnLicpLnBvcCgpOyAvLyByZXRyaWV2ZSBsYXR0ZXIgcGFydFxuICAgIGlmICghbXNnKSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoJ0ludmFsaWQgc2lnbmVkIHJlcXVlc3QnKTtcbiAgICB9XG4gICAgY29uc3QganNvbiA9IEJ1ZmZlci5mcm9tKG1zZywgJ2Jhc2U2NCcpLnRvU3RyaW5nKCd1dGYtOCcpO1xuICAgIHJldHVybiBKU09OLnBhcnNlKGpzb24pO1xuICB9XG4gIHJldHVybiBzciBhcyBTaWduZWRSZXF1ZXN0T2JqZWN0O1xufVxuXG4vKiogQHByaXZhdGUgKiovXG5mdW5jdGlvbiBwYXJzZUlkVXJsKHVybDogc3RyaW5nKSB7XG4gIGNvbnN0IFtvcmdhbml6YXRpb25JZCwgaWRdID0gdXJsLnNwbGl0KCcvJykuc2xpY2UoLTIpO1xuICByZXR1cm4geyBpZCwgb3JnYW5pemF0aW9uSWQsIHVybCB9O1xufVxuXG4vKipcbiAqIFNlc3Npb24gUmVmcmVzaCBkZWxlZ2F0ZSBmdW5jdGlvbiBmb3IgT0F1dGgyIGF1dGh6IGNvZGUgZmxvd1xuICogQHByaXZhdGVcbiAqL1xuYXN5bmMgZnVuY3Rpb24gb2F1dGhSZWZyZXNoRm48UyBleHRlbmRzIFNjaGVtYT4oXG4gIGNvbm46IENvbm5lY3Rpb248Uz4sXG4gIGNhbGxiYWNrOiBDYWxsYmFjazxzdHJpbmcsIFRva2VuUmVzcG9uc2U+LFxuKSB7XG4gIHRyeSB7XG4gICAgaWYgKCFjb25uLnJlZnJlc2hUb2tlbikge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKCdObyByZWZyZXNoIHRva2VuIGZvdW5kIGluIHRoZSBjb25uZWN0aW9uJyk7XG4gICAgfVxuICAgIGNvbnN0IHJlcyA9IGF3YWl0IGNvbm4ub2F1dGgyLnJlZnJlc2hUb2tlbihjb25uLnJlZnJlc2hUb2tlbik7XG4gICAgY29uc3QgdXNlckluZm8gPSBwYXJzZUlkVXJsKHJlcy5pZCk7XG4gICAgY29ubi5fZXN0YWJsaXNoKHtcbiAgICAgIGluc3RhbmNlVXJsOiByZXMuaW5zdGFuY2VfdXJsLFxuICAgICAgYWNjZXNzVG9rZW46IHJlcy5hY2Nlc3NfdG9rZW4sXG4gICAgICB1c2VySW5mbyxcbiAgICB9KTtcbiAgICBjYWxsYmFjayh1bmRlZmluZWQsIHJlcy5hY2Nlc3NfdG9rZW4sIHJlcyk7XG4gIH0gY2F0Y2ggKGVycikge1xuICAgIGlmIChlcnIgaW5zdGFuY2VvZiBFcnJvcikge1xuICAgICAgY2FsbGJhY2soZXJyKTtcbiAgICB9IGVsc2Uge1xuICAgICAgdGhyb3cgZXJyO1xuICAgIH1cbiAgfVxufVxuXG4vKipcbiAqIFNlc3Npb24gUmVmcmVzaCBkZWxlZ2F0ZSBmdW5jdGlvbiBmb3IgdXNlcm5hbWUvcGFzc3dvcmQgbG9naW5cbiAqIEBwcml2YXRlXG4gKi9cbmZ1bmN0aW9uIGNyZWF0ZVVzZXJuYW1lUGFzc3dvcmRSZWZyZXNoRm48UyBleHRlbmRzIFNjaGVtYT4oXG4gIHVzZXJuYW1lOiBzdHJpbmcsXG4gIHBhc3N3b3JkOiBzdHJpbmcsXG4pIHtcbiAgcmV0dXJuIGFzeW5jIChcbiAgICBjb25uOiBDb25uZWN0aW9uPFM+LFxuICAgIGNhbGxiYWNrOiBDYWxsYmFjazxzdHJpbmcsIFRva2VuUmVzcG9uc2U+LFxuICApID0+IHtcbiAgICB0cnkge1xuICAgICAgYXdhaXQgY29ubi5sb2dpbih1c2VybmFtZSwgcGFzc3dvcmQpO1xuICAgICAgaWYgKCFjb25uLmFjY2Vzc1Rva2VuKSB7XG4gICAgICAgIHRocm93IG5ldyBFcnJvcignQWNjZXNzIHRva2VuIG5vdCBmb3VuZCBhZnRlciBsb2dpbicpO1xuICAgICAgfVxuICAgICAgY2FsbGJhY2sobnVsbCwgY29ubi5hY2Nlc3NUb2tlbik7XG4gICAgfSBjYXRjaCAoZXJyKSB7XG4gICAgICBpZiAoZXJyIGluc3RhbmNlb2YgRXJyb3IpIHtcbiAgICAgICAgY2FsbGJhY2soZXJyKTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHRocm93IGVycjtcbiAgICAgIH1cbiAgICB9XG4gIH07XG59XG5cbi8qKlxuICogQHByaXZhdGVcbiAqL1xuZnVuY3Rpb24gdG9TYXZlUmVzdWx0KGVycjogU2F2ZUVycm9yKTogU2F2ZVJlc3VsdCB7XG4gIHJldHVybiB7XG4gICAgc3VjY2VzczogZmFsc2UsXG4gICAgZXJyb3JzOiBbZXJyXSxcbiAgfTtcbn1cblxuLyoqXG4gKlxuICovXG5mdW5jdGlvbiByYWlzZU5vTW9kdWxlRXJyb3IobmFtZTogc3RyaW5nKTogbmV2ZXIge1xuICB0aHJvdyBuZXcgRXJyb3IoXG4gICAgYEFQSSBtb2R1bGUgJyR7bmFtZX0nIGlzIG5vdCBsb2FkZWQsIGxvYWQgJ2pzZm9yY2UvYXBpLyR7bmFtZX0nIGV4cGxpY2l0bHlgLFxuICApO1xufVxuXG4vKlxuICogQ29uc3RhbnQgb2YgbWF4aW11bSByZWNvcmRzIG51bSBpbiBETUwgb3BlcmF0aW9uICh1cGRhdGUvZGVsZXRlKVxuICovXG5jb25zdCBNQVhfRE1MX0NPVU5UID0gMjAwO1xuXG4vKipcbiAqXG4gKi9cbmV4cG9ydCBjbGFzcyBDb25uZWN0aW9uPFMgZXh0ZW5kcyBTY2hlbWEgPSBTY2hlbWE+IGV4dGVuZHMgRXZlbnRFbWl0dGVyIHtcbiAgc3RhdGljIF9sb2dnZXIgPSBnZXRMb2dnZXIoJ2Nvbm5lY3Rpb24nKTtcblxuICB2ZXJzaW9uOiBzdHJpbmc7XG4gIGxvZ2luVXJsOiBzdHJpbmc7XG4gIGluc3RhbmNlVXJsOiBzdHJpbmc7XG4gIGFjY2Vzc1Rva2VuOiBPcHRpb25hbDxzdHJpbmc+O1xuICByZWZyZXNoVG9rZW46IE9wdGlvbmFsPHN0cmluZz47XG4gIHVzZXJJbmZvOiBPcHRpb25hbDxVc2VySW5mbz47XG4gIGxpbWl0SW5mbzogTGltaXRJbmZvID0ge307XG4gIG9hdXRoMjogT0F1dGgyO1xuICBzb2JqZWN0czogeyBbTiBpbiBTT2JqZWN0TmFtZXM8Uz5dPzogU09iamVjdDxTLCBOPiB9ID0ge307XG4gIGNhY2hlOiBDYWNoZTtcbiAgX2NhbGxPcHRpb25zOiBPcHRpb25hbDx7IFtuYW1lOiBzdHJpbmddOiBzdHJpbmcgfT47XG4gIF9tYXhSZXF1ZXN0OiBudW1iZXI7XG4gIF9sb2dnZXI6IExvZ2dlcjtcbiAgX2xvZ0xldmVsOiBPcHRpb25hbDxMb2dMZXZlbENvbmZpZz47XG4gIF90cmFuc3BvcnQ6IFRyYW5zcG9ydDtcbiAgX3Nlc3Npb25UeXBlOiBPcHRpb25hbDwnc29hcCcgfCAnb2F1dGgyJz47XG4gIF9yZWZyZXNoRGVsZWdhdGU6IE9wdGlvbmFsPFNlc3Npb25SZWZyZXNoRGVsZWdhdGU8Uz4+O1xuXG4gIC8vIGRlc2NyaWJlOiAobmFtZTogc3RyaW5nKSA9PiBQcm9taXNlPERlc2NyaWJlU09iamVjdFJlc3VsdD47XG4gIGRlc2NyaWJlJDogQ2FjaGVkRnVuY3Rpb248KG5hbWU6IHN0cmluZykgPT4gUHJvbWlzZTxEZXNjcmliZVNPYmplY3RSZXN1bHQ+PjtcbiAgZGVzY3JpYmUkJDogQ2FjaGVkRnVuY3Rpb248KG5hbWU6IHN0cmluZykgPT4gRGVzY3JpYmVTT2JqZWN0UmVzdWx0PjtcbiAgZGVzY3JpYmVTT2JqZWN0OiAobmFtZTogc3RyaW5nKSA9PiBQcm9taXNlPERlc2NyaWJlU09iamVjdFJlc3VsdD47XG4gIGRlc2NyaWJlU09iamVjdCQ6IENhY2hlZEZ1bmN0aW9uPFxuICAgIChuYW1lOiBzdHJpbmcpID0+IFByb21pc2U8RGVzY3JpYmVTT2JqZWN0UmVzdWx0PlxuICA+O1xuICBkZXNjcmliZVNPYmplY3QkJDogQ2FjaGVkRnVuY3Rpb248KG5hbWU6IHN0cmluZykgPT4gRGVzY3JpYmVTT2JqZWN0UmVzdWx0PjtcbiAgLy8gZGVzY3JpYmVHbG9iYWw6ICgpID0+IFByb21pc2U8RGVzY3JpYmVHbG9iYWxSZXN1bHQ+O1xuICBkZXNjcmliZUdsb2JhbCQ6IENhY2hlZEZ1bmN0aW9uPCgpID0+IFByb21pc2U8RGVzY3JpYmVHbG9iYWxSZXN1bHQ+PjtcbiAgZGVzY3JpYmVHbG9iYWwkJDogQ2FjaGVkRnVuY3Rpb248KCkgPT4gRGVzY3JpYmVHbG9iYWxSZXN1bHQ+O1xuXG4gIC8vIEFQSSBsaWJzIGFyZSBub3QgaW5zdGFudGlhdGVkIGhlcmUgc28gdGhhdCBjb3JlIG1vZHVsZSB0byByZW1haW4gd2l0aG91dCBkZXBlbmRlbmNpZXMgdG8gdGhlbVxuICAvLyBJdCBpcyByZXNwb25zaWJsZSBmb3IgZGV2ZWxvcGVycyB0byBpbXBvcnQgYXBpIGxpYnMgZXhwbGljaXRseSBpZiB0aGV5IGFyZSB1c2luZyAnanNmb3JjZS9jb3JlJyBpbnN0ZWFkIG9mICdqc2ZvcmNlJy5cbiAgZ2V0IGFuYWx5dGljcygpOiBBbmFseXRpY3M8Uz4ge1xuICAgIHJldHVybiByYWlzZU5vTW9kdWxlRXJyb3IoJ2FuYWx5dGljcycpO1xuICB9XG5cbiAgZ2V0IGFwZXgoKTogQXBleDxTPiB7XG4gICAgcmV0dXJuIHJhaXNlTm9Nb2R1bGVFcnJvcignYXBleCcpO1xuICB9XG5cbiAgZ2V0IGJ1bGsoKTogQnVsazxTPiB7XG4gICAgcmV0dXJuIHJhaXNlTm9Nb2R1bGVFcnJvcignYnVsaycpO1xuICB9XG5cbiAgZ2V0IGJ1bGsyKCk6IEJ1bGtWMjxTPiB7XG4gICAgcmV0dXJuIHJhaXNlTm9Nb2R1bGVFcnJvcignYnVsazInKTtcbiAgfVxuXG4gIGdldCBjaGF0dGVyKCk6IENoYXR0ZXI8Uz4ge1xuICAgIHJldHVybiByYWlzZU5vTW9kdWxlRXJyb3IoJ2NoYXR0ZXInKTtcbiAgfVxuXG4gIGdldCBtZXRhZGF0YSgpOiBNZXRhZGF0YTxTPiB7XG4gICAgcmV0dXJuIHJhaXNlTm9Nb2R1bGVFcnJvcignbWV0YWRhdGEnKTtcbiAgfVxuXG4gIGdldCBzb2FwKCk6IFNvYXBBcGk8Uz4ge1xuICAgIHJldHVybiByYWlzZU5vTW9kdWxlRXJyb3IoJ3NvYXAnKTtcbiAgfVxuXG4gIGdldCBzdHJlYW1pbmcoKTogU3RyZWFtaW5nPFM+IHtcbiAgICByZXR1cm4gcmFpc2VOb01vZHVsZUVycm9yKCdzdHJlYW1pbmcnKTtcbiAgfVxuXG4gIGdldCB0b29saW5nKCk6IFRvb2xpbmc8Uz4ge1xuICAgIHJldHVybiByYWlzZU5vTW9kdWxlRXJyb3IoJ3Rvb2xpbmcnKTtcbiAgfVxuXG4gIC8qKlxuICAgKlxuICAgKi9cbiAgY29uc3RydWN0b3IoY29uZmlnOiBDb25uZWN0aW9uQ29uZmlnPFM+ID0ge30pIHtcbiAgICBzdXBlcigpO1xuICAgIGNvbnN0IHtcbiAgICAgIGxvZ2luVXJsLFxuICAgICAgaW5zdGFuY2VVcmwsXG4gICAgICB2ZXJzaW9uLFxuICAgICAgb2F1dGgyLFxuICAgICAgbWF4UmVxdWVzdCxcbiAgICAgIGxvZ0xldmVsLFxuICAgICAgcHJveHlVcmwsXG4gICAgICBodHRwUHJveHksXG4gICAgfSA9IGNvbmZpZztcbiAgICB0aGlzLmxvZ2luVXJsID0gbG9naW5VcmwgfHwgZGVmYXVsdENvbm5lY3Rpb25Db25maWcubG9naW5Vcmw7XG4gICAgdGhpcy5pbnN0YW5jZVVybCA9IGluc3RhbmNlVXJsIHx8IGRlZmF1bHRDb25uZWN0aW9uQ29uZmlnLmluc3RhbmNlVXJsO1xuXG4gICAgaWYgKHRoaXMuaXNMaWdodG5pbmdJbnN0YW5jZSgpKSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoJ2xpZ2h0bmluZyBVUkxzIGFyZSBub3QgdmFsaWQgYXMgaW5zdGFuY2UgVVJMcycpO1xuICAgIH1cblxuICAgIHRoaXMudmVyc2lvbiA9IHZlcnNpb24gfHwgZGVmYXVsdENvbm5lY3Rpb25Db25maWcudmVyc2lvbjtcbiAgICB0aGlzLm9hdXRoMiA9XG4gICAgICBvYXV0aDIgaW5zdGFuY2VvZiBPQXV0aDJcbiAgICAgICAgPyBvYXV0aDJcbiAgICAgICAgOiBuZXcgT0F1dGgyKHtcbiAgICAgICAgICAgIGxvZ2luVXJsOiB0aGlzLmxvZ2luVXJsLFxuICAgICAgICAgICAgcHJveHlVcmwsXG4gICAgICAgICAgICBodHRwUHJveHksXG4gICAgICAgICAgICAuLi5vYXV0aDIsXG4gICAgICAgICAgfSk7XG4gICAgbGV0IHJlZnJlc2hGbiA9IGNvbmZpZy5yZWZyZXNoRm47XG4gICAgaWYgKCFyZWZyZXNoRm4gJiYgdGhpcy5vYXV0aDIuY2xpZW50SWQpIHtcbiAgICAgIHJlZnJlc2hGbiA9IG9hdXRoUmVmcmVzaEZuO1xuICAgIH1cbiAgICBpZiAocmVmcmVzaEZuKSB7XG4gICAgICB0aGlzLl9yZWZyZXNoRGVsZWdhdGUgPSBuZXcgU2Vzc2lvblJlZnJlc2hEZWxlZ2F0ZSh0aGlzLCByZWZyZXNoRm4pO1xuICAgIH1cbiAgICB0aGlzLl9tYXhSZXF1ZXN0ID0gbWF4UmVxdWVzdCB8fCBkZWZhdWx0Q29ubmVjdGlvbkNvbmZpZy5tYXhSZXF1ZXN0O1xuICAgIHRoaXMuX2xvZ2dlciA9IGxvZ0xldmVsXG4gICAgICA/IENvbm5lY3Rpb24uX2xvZ2dlci5jcmVhdGVJbnN0YW5jZShsb2dMZXZlbClcbiAgICAgIDogQ29ubmVjdGlvbi5fbG9nZ2VyO1xuICAgIHRoaXMuX2xvZ0xldmVsID0gbG9nTGV2ZWw7XG4gICAgdGhpcy5fdHJhbnNwb3J0ID0gcHJveHlVcmxcbiAgICAgID8gbmV3IFhkUHJveHlUcmFuc3BvcnQocHJveHlVcmwpXG4gICAgICA6IGh0dHBQcm94eVxuICAgICAgPyBuZXcgSHR0cFByb3h5VHJhbnNwb3J0KGh0dHBQcm94eSlcbiAgICAgIDogbmV3IFRyYW5zcG9ydCgpO1xuICAgIHRoaXMuX2NhbGxPcHRpb25zID0gY29uZmlnLmNhbGxPcHRpb25zO1xuICAgIHRoaXMuY2FjaGUgPSBuZXcgQ2FjaGUoKTtcbiAgICBjb25zdCBkZXNjcmliZUNhY2hlS2V5ID0gKHR5cGU/OiBzdHJpbmcpID0+XG4gICAgICB0eXBlID8gYGRlc2NyaWJlLiR7dHlwZX1gIDogJ2Rlc2NyaWJlJztcbiAgICBjb25zdCBkZXNjcmliZSA9IENvbm5lY3Rpb24ucHJvdG90eXBlLmRlc2NyaWJlO1xuICAgIHRoaXMuZGVzY3JpYmUgPSB0aGlzLmNhY2hlLmNyZWF0ZUNhY2hlZEZ1bmN0aW9uKGRlc2NyaWJlLCB0aGlzLCB7XG4gICAgICBrZXk6IGRlc2NyaWJlQ2FjaGVLZXksXG4gICAgICBzdHJhdGVneTogJ05PQ0FDSEUnLFxuICAgIH0pO1xuICAgIHRoaXMuZGVzY3JpYmUkID0gdGhpcy5jYWNoZS5jcmVhdGVDYWNoZWRGdW5jdGlvbihkZXNjcmliZSwgdGhpcywge1xuICAgICAga2V5OiBkZXNjcmliZUNhY2hlS2V5LFxuICAgICAgc3RyYXRlZ3k6ICdISVQnLFxuICAgIH0pO1xuICAgIHRoaXMuZGVzY3JpYmUkJCA9IHRoaXMuY2FjaGUuY3JlYXRlQ2FjaGVkRnVuY3Rpb24oZGVzY3JpYmUsIHRoaXMsIHtcbiAgICAgIGtleTogZGVzY3JpYmVDYWNoZUtleSxcbiAgICAgIHN0cmF0ZWd5OiAnSU1NRURJQVRFJyxcbiAgICB9KSBhcyBhbnk7XG4gICAgdGhpcy5kZXNjcmliZVNPYmplY3QgPSB0aGlzLmRlc2NyaWJlO1xuICAgIHRoaXMuZGVzY3JpYmVTT2JqZWN0JCA9IHRoaXMuZGVzY3JpYmUkO1xuICAgIHRoaXMuZGVzY3JpYmVTT2JqZWN0JCQgPSB0aGlzLmRlc2NyaWJlJCQ7XG4gICAgY29uc3QgZGVzY3JpYmVHbG9iYWwgPSBDb25uZWN0aW9uLnByb3RvdHlwZS5kZXNjcmliZUdsb2JhbDtcbiAgICB0aGlzLmRlc2NyaWJlR2xvYmFsID0gdGhpcy5jYWNoZS5jcmVhdGVDYWNoZWRGdW5jdGlvbihcbiAgICAgIGRlc2NyaWJlR2xvYmFsLFxuICAgICAgdGhpcyxcbiAgICAgIHsga2V5OiAnZGVzY3JpYmVHbG9iYWwnLCBzdHJhdGVneTogJ05PQ0FDSEUnIH0sXG4gICAgKTtcbiAgICB0aGlzLmRlc2NyaWJlR2xvYmFsJCA9IHRoaXMuY2FjaGUuY3JlYXRlQ2FjaGVkRnVuY3Rpb24oXG4gICAgICBkZXNjcmliZUdsb2JhbCxcbiAgICAgIHRoaXMsXG4gICAgICB7IGtleTogJ2Rlc2NyaWJlR2xvYmFsJywgc3RyYXRlZ3k6ICdISVQnIH0sXG4gICAgKTtcbiAgICB0aGlzLmRlc2NyaWJlR2xvYmFsJCQgPSB0aGlzLmNhY2hlLmNyZWF0ZUNhY2hlZEZ1bmN0aW9uKFxuICAgICAgZGVzY3JpYmVHbG9iYWwsXG4gICAgICB0aGlzLFxuICAgICAgeyBrZXk6ICdkZXNjcmliZUdsb2JhbCcsIHN0cmF0ZWd5OiAnSU1NRURJQVRFJyB9LFxuICAgICkgYXMgYW55O1xuICAgIGNvbnN0IHtcbiAgICAgIGFjY2Vzc1Rva2VuLFxuICAgICAgcmVmcmVzaFRva2VuLFxuICAgICAgc2Vzc2lvbklkLFxuICAgICAgc2VydmVyVXJsLFxuICAgICAgc2lnbmVkUmVxdWVzdCxcbiAgICB9ID0gY29uZmlnO1xuICAgIHRoaXMuX2VzdGFibGlzaCh7XG4gICAgICBhY2Nlc3NUb2tlbixcbiAgICAgIHJlZnJlc2hUb2tlbixcbiAgICAgIGluc3RhbmNlVXJsLFxuICAgICAgc2Vzc2lvbklkLFxuICAgICAgc2VydmVyVXJsLFxuICAgICAgc2lnbmVkUmVxdWVzdCxcbiAgICB9KTtcblxuICAgIGpzZm9yY2UuZW1pdCgnY29ubmVjdGlvbjpuZXcnLCB0aGlzKTtcbiAgfVxuXG4gIC8qIEBwcml2YXRlICovXG4gIF9lc3RhYmxpc2gob3B0aW9uczogQ29ubmVjdGlvbkVzdGFibGlzaE9wdGlvbnMpIHtcbiAgICBjb25zdCB7XG4gICAgICBhY2Nlc3NUb2tlbixcbiAgICAgIHJlZnJlc2hUb2tlbixcbiAgICAgIGluc3RhbmNlVXJsLFxuICAgICAgc2Vzc2lvbklkLFxuICAgICAgc2VydmVyVXJsLFxuICAgICAgc2lnbmVkUmVxdWVzdCxcbiAgICAgIHVzZXJJbmZvLFxuICAgIH0gPSBvcHRpb25zO1xuICAgIHRoaXMuaW5zdGFuY2VVcmwgPSBzZXJ2ZXJVcmxcbiAgICAgID8gc2VydmVyVXJsLnNwbGl0KCcvJykuc2xpY2UoMCwgMykuam9pbignLycpXG4gICAgICA6IGluc3RhbmNlVXJsIHx8IHRoaXMuaW5zdGFuY2VVcmw7XG4gICAgdGhpcy5hY2Nlc3NUb2tlbiA9IHNlc3Npb25JZCB8fCBhY2Nlc3NUb2tlbiB8fCB0aGlzLmFjY2Vzc1Rva2VuO1xuICAgIHRoaXMucmVmcmVzaFRva2VuID0gcmVmcmVzaFRva2VuIHx8IHRoaXMucmVmcmVzaFRva2VuO1xuICAgIGlmICh0aGlzLnJlZnJlc2hUb2tlbiAmJiAhdGhpcy5fcmVmcmVzaERlbGVnYXRlKSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoXG4gICAgICAgICdSZWZyZXNoIHRva2VuIGlzIHNwZWNpZmllZCB3aXRob3V0IG9hdXRoMiBjbGllbnQgaW5mb3JtYXRpb24gb3IgcmVmcmVzaCBmdW5jdGlvbicsXG4gICAgICApO1xuICAgIH1cbiAgICBjb25zdCBzaWduZWRSZXF1ZXN0T2JqZWN0ID1cbiAgICAgIHNpZ25lZFJlcXVlc3QgJiYgcGFyc2VTaWduZWRSZXF1ZXN0KHNpZ25lZFJlcXVlc3QpO1xuICAgIGlmIChzaWduZWRSZXF1ZXN0T2JqZWN0KSB7XG4gICAgICB0aGlzLmFjY2Vzc1Rva2VuID0gc2lnbmVkUmVxdWVzdE9iamVjdC5jbGllbnQub2F1dGhUb2tlbjtcbiAgICAgIGlmIChDYW52YXNUcmFuc3BvcnQuc3VwcG9ydGVkKSB7XG4gICAgICAgIHRoaXMuX3RyYW5zcG9ydCA9IG5ldyBDYW52YXNUcmFuc3BvcnQoc2lnbmVkUmVxdWVzdE9iamVjdCk7XG4gICAgICB9XG4gICAgfVxuICAgIHRoaXMudXNlckluZm8gPSB1c2VySW5mbyB8fCB0aGlzLnVzZXJJbmZvO1xuICAgIHRoaXMuX3Nlc3Npb25UeXBlID0gc2Vzc2lvbklkID8gJ3NvYXAnIDogJ29hdXRoMic7XG4gICAgdGhpcy5fcmVzZXRJbnN0YW5jZSgpO1xuICB9XG5cbiAgLyogQHByaXZlYXRlICovXG4gIF9jbGVhclNlc3Npb24oKSB7XG4gICAgdGhpcy5hY2Nlc3NUb2tlbiA9IG51bGw7XG4gICAgdGhpcy5yZWZyZXNoVG9rZW4gPSBudWxsO1xuICAgIHRoaXMuaW5zdGFuY2VVcmwgPSBkZWZhdWx0Q29ubmVjdGlvbkNvbmZpZy5pbnN0YW5jZVVybDtcbiAgICB0aGlzLnVzZXJJbmZvID0gbnVsbDtcbiAgICB0aGlzLl9zZXNzaW9uVHlwZSA9IG51bGw7XG4gIH1cblxuICAvKiBAcHJpdmVhdGUgKi9cbiAgX3Jlc2V0SW5zdGFuY2UoKSB7XG4gICAgdGhpcy5saW1pdEluZm8gPSB7fTtcbiAgICB0aGlzLnNvYmplY3RzID0ge307XG4gICAgLy8gVE9ETyBpbXBsIGNhY2hlXG4gICAgdGhpcy5jYWNoZS5jbGVhcigpO1xuICAgIHRoaXMuY2FjaGUuZ2V0KCdkZXNjcmliZUdsb2JhbCcpLnJlbW92ZUFsbExpc3RlbmVycygndmFsdWUnKTtcbiAgICB0aGlzLmNhY2hlLmdldCgnZGVzY3JpYmVHbG9iYWwnKS5vbigndmFsdWUnLCAoeyByZXN1bHQgfSkgPT4ge1xuICAgICAgaWYgKHJlc3VsdCkge1xuICAgICAgICBmb3IgKGNvbnN0IHNvIG9mIHJlc3VsdC5zb2JqZWN0cykge1xuICAgICAgICAgIHRoaXMuc29iamVjdChzby5uYW1lKTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH0pO1xuICAgIC8qXG4gICAgaWYgKHRoaXMudG9vbGluZykge1xuICAgICAgdGhpcy50b29saW5nLl9yZXNldEluc3RhbmNlKCk7XG4gICAgfVxuICAgICovXG4gIH1cblxuICAvKipcbiAgICogQXV0aG9yaXplIHRoZSBjb25uZWN0aW9uIHVzaW5nIE9BdXRoMiBmbG93LlxuICAgKiBUeXBpY2FsbHksIGp1c3QgcGFzcyB0aGUgY29kZSByZXR1cm5lZCBmcm9tIGF1dGhvcml6YXRpb24gc2VydmVyIGluIHRoZSBmaXJzdCBhcmd1bWVudCB0byBjb21wbGV0ZSBhdXRob3JpemF0aW9uLlxuICAgKiBJZiB5b3Ugd2FudCB0byBhdXRob3JpemUgd2l0aCBncmFudCB0eXBlcyBvdGhlciB0aGFuIGBhdXRob3JpemF0aW9uX2NvZGVgLCB5b3UgY2FuIGFsc28gcGFzcyBwYXJhbXMgb2JqZWN0IHdpdGggdGhlIGdyYW50IHR5cGUuXG4gICAqXG4gICAqIEByZXR1cm5zIHtQcm9taXNlPFVzZXJJbmZvPn0gQW4gb2JqZWN0IHRoYXQgY29udGFpbnMgdGhlIHVzZXIgSUQsIG9yZyBJRCBhbmQgaWRlbnRpdHkgVVJMLlxuICAgKlxuICAgKi9cbiAgYXN5bmMgYXV0aG9yaXplKFxuICAgIGNvZGVPclBhcmFtczogc3RyaW5nIHwgeyBncmFudF90eXBlOiBzdHJpbmc7IFtuYW1lOiBzdHJpbmddOiBzdHJpbmcgfSxcbiAgICBwYXJhbXM6IHsgW25hbWU6IHN0cmluZ106IHN0cmluZyB9ID0ge30sXG4gICk6IFByb21pc2U8VXNlckluZm8+IHtcbiAgICBjb25zdCByZXMgPSBhd2FpdCB0aGlzLm9hdXRoMi5yZXF1ZXN0VG9rZW4oY29kZU9yUGFyYW1zLCBwYXJhbXMpO1xuICAgIGNvbnN0IHVzZXJJbmZvID0gcGFyc2VJZFVybChyZXMuaWQpO1xuICAgIHRoaXMuX2VzdGFibGlzaCh7XG4gICAgICBpbnN0YW5jZVVybDogcmVzLmluc3RhbmNlX3VybCxcbiAgICAgIGFjY2Vzc1Rva2VuOiByZXMuYWNjZXNzX3Rva2VuLFxuICAgICAgcmVmcmVzaFRva2VuOiByZXMucmVmcmVzaF90b2tlbixcbiAgICAgIHVzZXJJbmZvLFxuICAgIH0pO1xuICAgIHRoaXMuX2xvZ2dlci5kZWJ1ZyhcbiAgICAgIGA8bG9naW4+IGNvbXBsZXRlZC4gdXNlciBpZCA9ICR7dXNlckluZm8uaWR9LCBvcmcgaWQgPSAke3VzZXJJbmZvLm9yZ2FuaXphdGlvbklkfWAsXG4gICAgKTtcbiAgICByZXR1cm4gdXNlckluZm87XG4gIH1cblxuICAvKipcbiAgICpcbiAgICovXG4gIGFzeW5jIGxvZ2luKHVzZXJuYW1lOiBzdHJpbmcsIHBhc3N3b3JkOiBzdHJpbmcpOiBQcm9taXNlPFVzZXJJbmZvPiB7XG4gICAgdGhpcy5fcmVmcmVzaERlbGVnYXRlID0gbmV3IFNlc3Npb25SZWZyZXNoRGVsZWdhdGUoXG4gICAgICB0aGlzLFxuICAgICAgY3JlYXRlVXNlcm5hbWVQYXNzd29yZFJlZnJlc2hGbih1c2VybmFtZSwgcGFzc3dvcmQpLFxuICAgICk7XG4gICAgaWYgKHRoaXMub2F1dGgyPy5jbGllbnRJZCAmJiB0aGlzLm9hdXRoMi5jbGllbnRTZWNyZXQpIHtcbiAgICAgIHJldHVybiB0aGlzLmxvZ2luQnlPQXV0aDIodXNlcm5hbWUsIHBhc3N3b3JkKTtcbiAgICB9XG4gICAgcmV0dXJuIHRoaXMubG9naW5CeVNvYXAodXNlcm5hbWUsIHBhc3N3b3JkKTtcbiAgfVxuXG4gIC8qKlxuICAgKiBMb2dpbiBieSBPQXV0aDIgdXNlcm5hbWUgJiBwYXNzd29yZCBmbG93XG4gICAqL1xuICBhc3luYyBsb2dpbkJ5T0F1dGgyKHVzZXJuYW1lOiBzdHJpbmcsIHBhc3N3b3JkOiBzdHJpbmcpOiBQcm9taXNlPFVzZXJJbmZvPiB7XG4gICAgY29uc3QgcmVzID0gYXdhaXQgdGhpcy5vYXV0aDIuYXV0aGVudGljYXRlKHVzZXJuYW1lLCBwYXNzd29yZCk7XG4gICAgY29uc3QgdXNlckluZm8gPSBwYXJzZUlkVXJsKHJlcy5pZCk7XG4gICAgdGhpcy5fZXN0YWJsaXNoKHtcbiAgICAgIGluc3RhbmNlVXJsOiByZXMuaW5zdGFuY2VfdXJsLFxuICAgICAgYWNjZXNzVG9rZW46IHJlcy5hY2Nlc3NfdG9rZW4sXG4gICAgICB1c2VySW5mbyxcbiAgICB9KTtcbiAgICB0aGlzLl9sb2dnZXIuaW5mbyhcbiAgICAgIGA8bG9naW4+IGNvbXBsZXRlZC4gdXNlciBpZCA9ICR7dXNlckluZm8uaWR9LCBvcmcgaWQgPSAke3VzZXJJbmZvLm9yZ2FuaXphdGlvbklkfWAsXG4gICAgKTtcbiAgICByZXR1cm4gdXNlckluZm87XG4gIH1cblxuICAvKipcbiAgICpcbiAgICovXG4gIGFzeW5jIGxvZ2luQnlTb2FwKHVzZXJuYW1lOiBzdHJpbmcsIHBhc3N3b3JkOiBzdHJpbmcpOiBQcm9taXNlPFVzZXJJbmZvPiB7XG4gICAgaWYgKCF1c2VybmFtZSB8fCAhcGFzc3dvcmQpIHtcbiAgICAgIHJldHVybiBQcm9taXNlLnJlamVjdChuZXcgRXJyb3IoJ25vIHVzZXJuYW1lIHBhc3N3b3JkIGdpdmVuJykpO1xuICAgIH1cbiAgICBjb25zdCBib2R5ID0gW1xuICAgICAgJzxzZTpFbnZlbG9wZSB4bWxuczpzZT1cImh0dHA6Ly9zY2hlbWFzLnhtbHNvYXAub3JnL3NvYXAvZW52ZWxvcGUvXCI+JyxcbiAgICAgICc8c2U6SGVhZGVyLz4nLFxuICAgICAgJzxzZTpCb2R5PicsXG4gICAgICAnPGxvZ2luIHhtbG5zPVwidXJuOnBhcnRuZXIuc29hcC5zZm9yY2UuY29tXCI+JyxcbiAgICAgIGA8dXNlcm5hbWU+JHtlc2ModXNlcm5hbWUpfTwvdXNlcm5hbWU+YCxcbiAgICAgIGA8cGFzc3dvcmQ+JHtlc2MocGFzc3dvcmQpfTwvcGFzc3dvcmQ+YCxcbiAgICAgICc8L2xvZ2luPicsXG4gICAgICAnPC9zZTpCb2R5PicsXG4gICAgICAnPC9zZTpFbnZlbG9wZT4nLFxuICAgIF0uam9pbignJyk7XG5cbiAgICBjb25zdCBzb2FwTG9naW5FbmRwb2ludCA9IFtcbiAgICAgIHRoaXMubG9naW5VcmwsXG4gICAgICAnc2VydmljZXMvU29hcC91JyxcbiAgICAgIHRoaXMudmVyc2lvbixcbiAgICBdLmpvaW4oJy8nKTtcbiAgICBjb25zdCByZXNwb25zZSA9IGF3YWl0IHRoaXMuX3RyYW5zcG9ydC5odHRwUmVxdWVzdCh7XG4gICAgICBtZXRob2Q6ICdQT1NUJyxcbiAgICAgIHVybDogc29hcExvZ2luRW5kcG9pbnQsXG4gICAgICBib2R5LFxuICAgICAgaGVhZGVyczoge1xuICAgICAgICAnQ29udGVudC1UeXBlJzogJ3RleHQveG1sJyxcbiAgICAgICAgU09BUEFjdGlvbjogJ1wiXCInLFxuICAgICAgfSxcbiAgICB9KTtcbiAgICBsZXQgbTtcbiAgICBpZiAocmVzcG9uc2Uuc3RhdHVzQ29kZSA+PSA0MDApIHtcbiAgICAgIG0gPSByZXNwb25zZS5ib2R5Lm1hdGNoKC88ZmF1bHRzdHJpbmc+KFtePF0rKTxcXC9mYXVsdHN0cmluZz4vKTtcbiAgICAgIGNvbnN0IGZhdWx0c3RyaW5nID0gbSAmJiBtWzFdO1xuICAgICAgdGhyb3cgbmV3IEVycm9yKGZhdWx0c3RyaW5nIHx8IHJlc3BvbnNlLmJvZHkpO1xuICAgIH1cbiAgICAvLyB0aGUgQVBJIHdpbGwgcmV0dXJuIDIwMCBhbmQgYSByZXN0cmljZWQgdG9rZW4gd2hlbiB1c2luZyBhbiBleHBpcmVkIHBhc3N3b3JkOlxuICAgIC8vIGh0dHBzOi8vZGV2ZWxvcGVyLnNhbGVzZm9yY2UuY29tL2RvY3MvYXRsYXMuZW4tdXMuYXBpLm1ldGEvYXBpL3Nmb3JjZV9hcGlfY2FsbHNfbG9naW5fbG9naW5yZXN1bHQuaHRtXG4gICAgLy9cbiAgICAvLyB3ZSBuZWVkIHRvIHRocm93IGhlcmUgdG8gYXZvaWQgYSBwb3NzaWJsZSBpbmZpbml0ZSBsb29wIHdpdGggc2Vzc2lvbiByZWZyZXNoIHdoZXJlOlxuICAgIC8vICAxLiBsb2dpbiBoYXBwZW5zLCBgdGhpcy5hY2Nlc3NUb2tlbmAgaXMgc2V0IHRvIHRoZSByZXN0cmljdGVkIHRva2VuXG4gICAgLy8gIDIuIHJlcXVlc3RzIGhhcHBlbiwgZ2V0IGJhY2sgNDAxXG4gICAgLy8gIDMuIHRyaWdnZXIgc2Vzc2lvbi1yZWZyZXNoICh1c2VybmFtZS9wYXNzd29yZCBsb2dpbiBoYXMgYSBkZWZhdWx0IHNlc3Npb24gcmVmcmVzaCBkZWxlZ2F0ZSBmdW5jdGlvbilcbiAgICAvLyAgNC4gZ2V0cyBzdHVjayByZWZyZXNoaW5nIGEgcmVzdHJpY3RlZCB0b2tlblxuICAgIGlmIChyZXNwb25zZS5ib2R5Lm1hdGNoKC88cGFzc3dvcmRFeHBpcmVkPnRydWU8XFwvcGFzc3dvcmRFeHBpcmVkPi9nKSkge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKCdVbmFibGUgdG8gbG9naW4gYmVjYXVzZSB0aGUgdXNlZCBwYXNzd29yZCBoYXMgZXhwaXJlZC4nKVxuICAgIH1cbiAgICB0aGlzLl9sb2dnZXIuZGVidWcoYFNPQVAgcmVzcG9uc2UgPSAke3Jlc3BvbnNlLmJvZHl9YCk7XG4gICAgbSA9IHJlc3BvbnNlLmJvZHkubWF0Y2goLzxzZXJ2ZXJVcmw+KFtePF0rKTxcXC9zZXJ2ZXJVcmw+Lyk7XG4gICAgY29uc3Qgc2VydmVyVXJsID0gbSAmJiBtWzFdO1xuICAgIG0gPSByZXNwb25zZS5ib2R5Lm1hdGNoKC88c2Vzc2lvbklkPihbXjxdKyk8XFwvc2Vzc2lvbklkPi8pO1xuICAgIGNvbnN0IHNlc3Npb25JZCA9IG0gJiYgbVsxXTtcbiAgICBtID0gcmVzcG9uc2UuYm9keS5tYXRjaCgvPHVzZXJJZD4oW148XSspPFxcL3VzZXJJZD4vKTtcbiAgICBjb25zdCB1c2VySWQgPSBtICYmIG1bMV07XG4gICAgbSA9IHJlc3BvbnNlLmJvZHkubWF0Y2goLzxvcmdhbml6YXRpb25JZD4oW148XSspPFxcL29yZ2FuaXphdGlvbklkPi8pO1xuICAgIGNvbnN0IG9yZ2FuaXphdGlvbklkID0gbSAmJiBtWzFdO1xuICAgIGlmICghc2VydmVyVXJsIHx8ICFzZXNzaW9uSWQgfHwgIXVzZXJJZCB8fCAhb3JnYW5pemF0aW9uSWQpIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcihcbiAgICAgICAgJ2NvdWxkIG5vdCBleHRyYWN0IHNlc3Npb24gaW5mb3JtYXRpb24gZnJvbSBsb2dpbiByZXNwb25zZScsXG4gICAgICApO1xuICAgIH1cbiAgICBjb25zdCBpZFVybCA9IFt0aGlzLmxvZ2luVXJsLCAnaWQnLCBvcmdhbml6YXRpb25JZCwgdXNlcklkXS5qb2luKCcvJyk7XG4gICAgY29uc3QgdXNlckluZm8gPSB7IGlkOiB1c2VySWQsIG9yZ2FuaXphdGlvbklkLCB1cmw6IGlkVXJsIH07XG4gICAgdGhpcy5fZXN0YWJsaXNoKHtcbiAgICAgIHNlcnZlclVybDogc2VydmVyVXJsLnNwbGl0KCcvJykuc2xpY2UoMCwgMykuam9pbignLycpLFxuICAgICAgc2Vzc2lvbklkLFxuICAgICAgdXNlckluZm8sXG4gICAgfSk7XG4gICAgdGhpcy5fbG9nZ2VyLmluZm8oXG4gICAgICBgPGxvZ2luPiBjb21wbGV0ZWQuIHVzZXIgaWQgPSAke3VzZXJJZH0sIG9yZyBpZCA9ICR7b3JnYW5pemF0aW9uSWR9YCxcbiAgICApO1xuICAgIHJldHVybiB1c2VySW5mbztcbiAgfVxuXG4gIC8qKlxuICAgKiBMb2dvdXQgdGhlIGN1cnJlbnQgc2Vzc2lvblxuICAgKi9cbiAgYXN5bmMgbG9nb3V0KHJldm9rZT86IGJvb2xlYW4pOiBQcm9taXNlPHZvaWQ+IHtcbiAgICB0aGlzLl9yZWZyZXNoRGVsZWdhdGUgPSB1bmRlZmluZWQ7XG4gICAgaWYgKHRoaXMuX3Nlc3Npb25UeXBlID09PSAnb2F1dGgyJykge1xuICAgICAgcmV0dXJuIHRoaXMubG9nb3V0QnlPQXV0aDIocmV2b2tlKTtcbiAgICB9XG4gICAgcmV0dXJuIHRoaXMubG9nb3V0QnlTb2FwKHJldm9rZSk7XG4gIH1cblxuICAvKipcbiAgICogTG9nb3V0IHRoZSBjdXJyZW50IHNlc3Npb24gYnkgcmV2b2tpbmcgYWNjZXNzIHRva2VuIHZpYSBPQXV0aDIgc2Vzc2lvbiByZXZva2VcbiAgICovXG4gIGFzeW5jIGxvZ291dEJ5T0F1dGgyKHJldm9rZT86IGJvb2xlYW4pOiBQcm9taXNlPHZvaWQ+IHtcbiAgICBjb25zdCB0b2tlbiA9IHJldm9rZSA/IHRoaXMucmVmcmVzaFRva2VuIDogdGhpcy5hY2Nlc3NUb2tlbjtcbiAgICBpZiAodG9rZW4pIHtcbiAgICAgIGF3YWl0IHRoaXMub2F1dGgyLnJldm9rZVRva2VuKHRva2VuKTtcbiAgICB9XG4gICAgLy8gRGVzdHJveSB0aGUgc2Vzc2lvbiBib3VuZCB0byB0aGlzIGNvbm5lY3Rpb25cbiAgICB0aGlzLl9jbGVhclNlc3Npb24oKTtcbiAgICB0aGlzLl9yZXNldEluc3RhbmNlKCk7XG4gIH1cblxuICAvKipcbiAgICogTG9nb3V0IHRoZSBzZXNzaW9uIGJ5IHVzaW5nIFNPQVAgd2ViIHNlcnZpY2UgQVBJXG4gICAqL1xuICBhc3luYyBsb2dvdXRCeVNvYXAocmV2b2tlPzogYm9vbGVhbik6IFByb21pc2U8dm9pZD4ge1xuICAgIGNvbnN0IGJvZHkgPSBbXG4gICAgICAnPHNlOkVudmVsb3BlIHhtbG5zOnNlPVwiaHR0cDovL3NjaGVtYXMueG1sc29hcC5vcmcvc29hcC9lbnZlbG9wZS9cIj4nLFxuICAgICAgJzxzZTpIZWFkZXI+JyxcbiAgICAgICc8U2Vzc2lvbkhlYWRlciB4bWxucz1cInVybjpwYXJ0bmVyLnNvYXAuc2ZvcmNlLmNvbVwiPicsXG4gICAgICBgPHNlc3Npb25JZD4ke2VzYyhcbiAgICAgICAgcmV2b2tlID8gdGhpcy5yZWZyZXNoVG9rZW4gOiB0aGlzLmFjY2Vzc1Rva2VuLFxuICAgICAgKX08L3Nlc3Npb25JZD5gLFxuICAgICAgJzwvU2Vzc2lvbkhlYWRlcj4nLFxuICAgICAgJzwvc2U6SGVhZGVyPicsXG4gICAgICAnPHNlOkJvZHk+JyxcbiAgICAgICc8bG9nb3V0IHhtbG5zPVwidXJuOnBhcnRuZXIuc29hcC5zZm9yY2UuY29tXCIvPicsXG4gICAgICAnPC9zZTpCb2R5PicsXG4gICAgICAnPC9zZTpFbnZlbG9wZT4nLFxuICAgIF0uam9pbignJyk7XG4gICAgY29uc3QgcmVzcG9uc2UgPSBhd2FpdCB0aGlzLl90cmFuc3BvcnQuaHR0cFJlcXVlc3Qoe1xuICAgICAgbWV0aG9kOiAnUE9TVCcsXG4gICAgICB1cmw6IFt0aGlzLmluc3RhbmNlVXJsLCAnc2VydmljZXMvU29hcC91JywgdGhpcy52ZXJzaW9uXS5qb2luKCcvJyksXG4gICAgICBib2R5LFxuICAgICAgaGVhZGVyczoge1xuICAgICAgICAnQ29udGVudC1UeXBlJzogJ3RleHQveG1sJyxcbiAgICAgICAgU09BUEFjdGlvbjogJ1wiXCInLFxuICAgICAgfSxcbiAgICB9KTtcbiAgICB0aGlzLl9sb2dnZXIuZGVidWcoXG4gICAgICBgU09BUCBzdGF0dXNDb2RlID0gJHtyZXNwb25zZS5zdGF0dXNDb2RlfSwgcmVzcG9uc2UgPSAke3Jlc3BvbnNlLmJvZHl9YCxcbiAgICApO1xuICAgIGlmIChyZXNwb25zZS5zdGF0dXNDb2RlID49IDQwMCkge1xuICAgICAgY29uc3QgbSA9IHJlc3BvbnNlLmJvZHkubWF0Y2goLzxmYXVsdHN0cmluZz4oW148XSspPFxcL2ZhdWx0c3RyaW5nPi8pO1xuICAgICAgY29uc3QgZmF1bHRzdHJpbmcgPSBtICYmIG1bMV07XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoZmF1bHRzdHJpbmcgfHwgcmVzcG9uc2UuYm9keSk7XG4gICAgfVxuICAgIC8vIERlc3Ryb3kgdGhlIHNlc3Npb24gYm91bmQgdG8gdGhpcyBjb25uZWN0aW9uXG4gICAgdGhpcy5fY2xlYXJTZXNzaW9uKCk7XG4gICAgdGhpcy5fcmVzZXRJbnN0YW5jZSgpO1xuICB9XG5cbiAgLyoqXG4gICAqIFNlbmQgUkVTVCBBUEkgcmVxdWVzdCB3aXRoIGdpdmVuIEhUVFAgcmVxdWVzdCBpbmZvLCB3aXRoIGNvbm5lY3RlZCBzZXNzaW9uIGluZm9ybWF0aW9uLlxuICAgKlxuICAgKiBFbmRwb2ludCBVUkwgY2FuIGJlIGFic29sdXRlIFVSTCAoJ2h0dHBzOi8vbmExLnNhbGVzZm9yY2UuY29tL3NlcnZpY2VzL2RhdGEvdjMyLjAvc29iamVjdHMvQWNjb3VudC9kZXNjcmliZScpXG4gICAqICwgcmVsYXRpdmUgcGF0aCBmcm9tIHJvb3QgKCcvc2VydmljZXMvZGF0YS92MzIuMC9zb2JqZWN0cy9BY2NvdW50L2Rlc2NyaWJlJylcbiAgICogLCBvciByZWxhdGl2ZSBwYXRoIGZyb20gdmVyc2lvbiByb290ICgnL3NvYmplY3RzL0FjY291bnQvZGVzY3JpYmUnKS5cbiAgICovXG4gIHJlcXVlc3Q8UiA9IHVua25vd24+KFxuICAgIHJlcXVlc3Q6IHN0cmluZyB8IEh0dHBSZXF1ZXN0LFxuICAgIG9wdGlvbnM6IE9iamVjdCA9IHt9LFxuICApOiBTdHJlYW1Qcm9taXNlPFI+IHtcbiAgICAvLyBpZiByZXF1ZXN0IGlzIHNpbXBsZSBzdHJpbmcsIHJlZ2FyZCBpdCBhcyB1cmwgaW4gR0VUIG1ldGhvZFxuICAgIGxldCByZXF1ZXN0XzogSHR0cFJlcXVlc3QgPVxuICAgICAgdHlwZW9mIHJlcXVlc3QgPT09ICdzdHJpbmcnID8geyBtZXRob2Q6ICdHRVQnLCB1cmw6IHJlcXVlc3QgfSA6IHJlcXVlc3Q7XG4gICAgLy8gaWYgdXJsIGlzIGdpdmVuIGluIHJlbGF0aXZlIHBhdGgsIHByZXBlbmQgYmFzZSB1cmwgb3IgaW5zdGFuY2UgdXJsIGJlZm9yZS5cbiAgICByZXF1ZXN0XyA9IHtcbiAgICAgIC4uLnJlcXVlc3RfLFxuICAgICAgdXJsOiB0aGlzLl9ub3JtYWxpemVVcmwocmVxdWVzdF8udXJsKSxcbiAgICB9O1xuICAgIGNvbnN0IGh0dHBBcGkgPSBuZXcgSHR0cEFwaSh0aGlzLCBvcHRpb25zKTtcbiAgICAvLyBsb2cgYXBpIHVzYWdlIGFuZCBpdHMgcXVvdGFcbiAgICBodHRwQXBpLm9uKCdyZXNwb25zZScsIChyZXNwb25zZTogSHR0cFJlc3BvbnNlKSA9PiB7XG4gICAgICBpZiAocmVzcG9uc2UuaGVhZGVycyAmJiByZXNwb25zZS5oZWFkZXJzWydzZm9yY2UtbGltaXQtaW5mbyddKSB7XG4gICAgICAgIGNvbnN0IGFwaVVzYWdlID0gcmVzcG9uc2UuaGVhZGVyc1snc2ZvcmNlLWxpbWl0LWluZm8nXS5tYXRjaChcbiAgICAgICAgICAvYXBpLXVzYWdlPShcXGQrKVxcLyhcXGQrKS8sXG4gICAgICAgICk7XG4gICAgICAgIGlmIChhcGlVc2FnZSkge1xuICAgICAgICAgIHRoaXMubGltaXRJbmZvID0ge1xuICAgICAgICAgICAgYXBpVXNhZ2U6IHtcbiAgICAgICAgICAgICAgdXNlZDogcGFyc2VJbnQoYXBpVXNhZ2VbMV0sIDEwKSxcbiAgICAgICAgICAgICAgbGltaXQ6IHBhcnNlSW50KGFwaVVzYWdlWzJdLCAxMCksXG4gICAgICAgICAgICB9LFxuICAgICAgICAgIH07XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9KTtcbiAgICByZXR1cm4gaHR0cEFwaS5yZXF1ZXN0PFI+KHJlcXVlc3RfKTtcbiAgfVxuXG4gIC8qKlxuICAgKiBTZW5kIEhUVFAgR0VUIHJlcXVlc3RcbiAgICpcbiAgICogRW5kcG9pbnQgVVJMIGNhbiBiZSBhYnNvbHV0ZSBVUkwgKCdodHRwczovL25hMS5zYWxlc2ZvcmNlLmNvbS9zZXJ2aWNlcy9kYXRhL3YzMi4wL3NvYmplY3RzL0FjY291bnQvZGVzY3JpYmUnKVxuICAgKiAsIHJlbGF0aXZlIHBhdGggZnJvbSByb290ICgnL3NlcnZpY2VzL2RhdGEvdjMyLjAvc29iamVjdHMvQWNjb3VudC9kZXNjcmliZScpXG4gICAqICwgb3IgcmVsYXRpdmUgcGF0aCBmcm9tIHZlcnNpb24gcm9vdCAoJy9zb2JqZWN0cy9BY2NvdW50L2Rlc2NyaWJlJykuXG4gICAqL1xuICByZXF1ZXN0R2V0PFIgPSB1bmtub3duPih1cmw6IHN0cmluZywgb3B0aW9ucz86IE9iamVjdCkge1xuICAgIGNvbnN0IHJlcXVlc3Q6IEh0dHBSZXF1ZXN0ID0geyBtZXRob2Q6ICdHRVQnLCB1cmwgfTtcbiAgICByZXR1cm4gdGhpcy5yZXF1ZXN0PFI+KHJlcXVlc3QsIG9wdGlvbnMpO1xuICB9XG5cbiAgLyoqXG4gICAqIFNlbmQgSFRUUCBQT1NUIHJlcXVlc3Qgd2l0aCBKU09OIGJvZHksIHdpdGggY29ubmVjdGVkIHNlc3Npb24gaW5mb3JtYXRpb25cbiAgICpcbiAgICogRW5kcG9pbnQgVVJMIGNhbiBiZSBhYnNvbHV0ZSBVUkwgKCdodHRwczovL25hMS5zYWxlc2ZvcmNlLmNvbS9zZXJ2aWNlcy9kYXRhL3YzMi4wL3NvYmplY3RzL0FjY291bnQvZGVzY3JpYmUnKVxuICAgKiAsIHJlbGF0aXZlIHBhdGggZnJvbSByb290ICgnL3NlcnZpY2VzL2RhdGEvdjMyLjAvc29iamVjdHMvQWNjb3VudC9kZXNjcmliZScpXG4gICAqICwgb3IgcmVsYXRpdmUgcGF0aCBmcm9tIHZlcnNpb24gcm9vdCAoJy9zb2JqZWN0cy9BY2NvdW50L2Rlc2NyaWJlJykuXG4gICAqL1xuICByZXF1ZXN0UG9zdDxSID0gdW5rbm93bj4odXJsOiBzdHJpbmcsIGJvZHk6IE9iamVjdCwgb3B0aW9ucz86IE9iamVjdCkge1xuICAgIGNvbnN0IHJlcXVlc3Q6IEh0dHBSZXF1ZXN0ID0ge1xuICAgICAgbWV0aG9kOiAnUE9TVCcsXG4gICAgICB1cmwsXG4gICAgICBib2R5OiBKU09OLnN0cmluZ2lmeShib2R5KSxcbiAgICAgIGhlYWRlcnM6IHsgJ2NvbnRlbnQtdHlwZSc6ICdhcHBsaWNhdGlvbi9qc29uJyB9LFxuICAgIH07XG4gICAgcmV0dXJuIHRoaXMucmVxdWVzdDxSPihyZXF1ZXN0LCBvcHRpb25zKTtcbiAgfVxuXG4gIC8qKlxuICAgKiBTZW5kIEhUVFAgUFVUIHJlcXVlc3Qgd2l0aCBKU09OIGJvZHksIHdpdGggY29ubmVjdGVkIHNlc3Npb24gaW5mb3JtYXRpb25cbiAgICpcbiAgICogRW5kcG9pbnQgVVJMIGNhbiBiZSBhYnNvbHV0ZSBVUkwgKCdodHRwczovL25hMS5zYWxlc2ZvcmNlLmNvbS9zZXJ2aWNlcy9kYXRhL3YzMi4wL3NvYmplY3RzL0FjY291bnQvZGVzY3JpYmUnKVxuICAgKiAsIHJlbGF0aXZlIHBhdGggZnJvbSByb290ICgnL3NlcnZpY2VzL2RhdGEvdjMyLjAvc29iamVjdHMvQWNjb3VudC9kZXNjcmliZScpXG4gICAqICwgb3IgcmVsYXRpdmUgcGF0aCBmcm9tIHZlcnNpb24gcm9vdCAoJy9zb2JqZWN0cy9BY2NvdW50L2Rlc2NyaWJlJykuXG4gICAqL1xuICByZXF1ZXN0UHV0PFI+KHVybDogc3RyaW5nLCBib2R5OiBPYmplY3QsIG9wdGlvbnM/OiBPYmplY3QpIHtcbiAgICBjb25zdCByZXF1ZXN0OiBIdHRwUmVxdWVzdCA9IHtcbiAgICAgIG1ldGhvZDogJ1BVVCcsXG4gICAgICB1cmwsXG4gICAgICBib2R5OiBKU09OLnN0cmluZ2lmeShib2R5KSxcbiAgICAgIGhlYWRlcnM6IHsgJ2NvbnRlbnQtdHlwZSc6ICdhcHBsaWNhdGlvbi9qc29uJyB9LFxuICAgIH07XG4gICAgcmV0dXJuIHRoaXMucmVxdWVzdDxSPihyZXF1ZXN0LCBvcHRpb25zKTtcbiAgfVxuXG4gIC8qKlxuICAgKiBTZW5kIEhUVFAgUEFUQ0ggcmVxdWVzdCB3aXRoIEpTT04gYm9keVxuICAgKlxuICAgKiBFbmRwb2ludCBVUkwgY2FuIGJlIGFic29sdXRlIFVSTCAoJ2h0dHBzOi8vbmExLnNhbGVzZm9yY2UuY29tL3NlcnZpY2VzL2RhdGEvdjMyLjAvc29iamVjdHMvQWNjb3VudC9kZXNjcmliZScpXG4gICAqICwgcmVsYXRpdmUgcGF0aCBmcm9tIHJvb3QgKCcvc2VydmljZXMvZGF0YS92MzIuMC9zb2JqZWN0cy9BY2NvdW50L2Rlc2NyaWJlJylcbiAgICogLCBvciByZWxhdGl2ZSBwYXRoIGZyb20gdmVyc2lvbiByb290ICgnL3NvYmplY3RzL0FjY291bnQvZGVzY3JpYmUnKS5cbiAgICovXG4gIHJlcXVlc3RQYXRjaDxSID0gdW5rbm93bj4odXJsOiBzdHJpbmcsIGJvZHk6IE9iamVjdCwgb3B0aW9ucz86IE9iamVjdCkge1xuICAgIGNvbnN0IHJlcXVlc3Q6IEh0dHBSZXF1ZXN0ID0ge1xuICAgICAgbWV0aG9kOiAnUEFUQ0gnLFxuICAgICAgdXJsLFxuICAgICAgYm9keTogSlNPTi5zdHJpbmdpZnkoYm9keSksXG4gICAgICBoZWFkZXJzOiB7ICdjb250ZW50LXR5cGUnOiAnYXBwbGljYXRpb24vanNvbicgfSxcbiAgICB9O1xuICAgIHJldHVybiB0aGlzLnJlcXVlc3Q8Uj4ocmVxdWVzdCwgb3B0aW9ucyk7XG4gIH1cblxuICAvKipcbiAgICogU2VuZCBIVFRQIERFTEVURSByZXF1ZXN0XG4gICAqXG4gICAqIEVuZHBvaW50IFVSTCBjYW4gYmUgYWJzb2x1dGUgVVJMICgnaHR0cHM6Ly9uYTEuc2FsZXNmb3JjZS5jb20vc2VydmljZXMvZGF0YS92MzIuMC9zb2JqZWN0cy9BY2NvdW50L2Rlc2NyaWJlJylcbiAgICogLCByZWxhdGl2ZSBwYXRoIGZyb20gcm9vdCAoJy9zZXJ2aWNlcy9kYXRhL3YzMi4wL3NvYmplY3RzL0FjY291bnQvZGVzY3JpYmUnKVxuICAgKiAsIG9yIHJlbGF0aXZlIHBhdGggZnJvbSB2ZXJzaW9uIHJvb3QgKCcvc29iamVjdHMvQWNjb3VudC9kZXNjcmliZScpLlxuICAgKi9cbiAgcmVxdWVzdERlbGV0ZTxSPih1cmw6IHN0cmluZywgb3B0aW9ucz86IE9iamVjdCkge1xuICAgIGNvbnN0IHJlcXVlc3Q6IEh0dHBSZXF1ZXN0ID0geyBtZXRob2Q6ICdERUxFVEUnLCB1cmwgfTtcbiAgICByZXR1cm4gdGhpcy5yZXF1ZXN0PFI+KHJlcXVlc3QsIG9wdGlvbnMpO1xuICB9XG5cbiAgLyoqIEBwcml2YXRlICoqL1xuICBfYmFzZVVybCgpIHtcbiAgICByZXR1cm4gW3RoaXMuaW5zdGFuY2VVcmwsICdzZXJ2aWNlcy9kYXRhJywgYHYke3RoaXMudmVyc2lvbn1gXS5qb2luKCcvJyk7XG4gIH1cblxuICAvKipcbiAgICogQ29udmVydCBwYXRoIHRvIGFic29sdXRlIHVybFxuICAgKiBAcHJpdmF0ZVxuICAgKi9cbiAgX25vcm1hbGl6ZVVybCh1cmw6IHN0cmluZykge1xuICAgIGlmICh1cmwuc3RhcnRzV2l0aCgnLycpKSB7XG4gICAgICBpZiAodXJsLnN0YXJ0c1dpdGgodGhpcy5pbnN0YW5jZVVybCArICcvc2VydmljZXMvJykpIHtcbiAgICAgICAgcmV0dXJuIHVybDtcbiAgICAgIH1cbiAgICAgIGlmICh1cmwuc3RhcnRzV2l0aCgnL3NlcnZpY2VzLycpKSB7XG4gICAgICAgIHJldHVybiB0aGlzLmluc3RhbmNlVXJsICsgdXJsO1xuICAgICAgfVxuICAgICAgcmV0dXJuIHRoaXMuX2Jhc2VVcmwoKSArIHVybDtcbiAgICB9XG4gICAgcmV0dXJuIHVybDtcbiAgfVxuXG4gIC8qKlxuICAgKlxuICAgKi9cbiAgcXVlcnk8VCBleHRlbmRzIFJlY29yZD4oXG4gICAgc29xbDogc3RyaW5nLFxuICAgIG9wdGlvbnM/OiBQYXJ0aWFsPFF1ZXJ5T3B0aW9ucz4sXG4gICk6IFF1ZXJ5PFMsIFNPYmplY3ROYW1lczxTPiwgVCwgJ1F1ZXJ5UmVzdWx0Jz4ge1xuICAgIHJldHVybiBuZXcgUXVlcnk8UywgU09iamVjdE5hbWVzPFM+LCBULCAnUXVlcnlSZXN1bHQnPih0aGlzLCBzb3FsLCBvcHRpb25zKTtcbiAgfVxuXG4gIC8qKlxuICAgKiBFeGVjdXRlIHNlYXJjaCBieSBTT1NMXG4gICAqXG4gICAqIEBwYXJhbSB7U3RyaW5nfSBzb3NsIC0gU09TTCBzdHJpbmdcbiAgICogQHBhcmFtIHtDYWxsYmFjay48QXJyYXkuPFJlY29yZFJlc3VsdD4+fSBbY2FsbGJhY2tdIC0gQ2FsbGJhY2sgZnVuY3Rpb25cbiAgICogQHJldHVybnMge1Byb21pc2UuPEFycmF5LjxSZWNvcmRSZXN1bHQ+Pn1cbiAgICovXG4gIHNlYXJjaChzb3NsOiBzdHJpbmcpIHtcbiAgICBjb25zdCB1cmwgPSB0aGlzLl9iYXNlVXJsKCkgKyAnL3NlYXJjaD9xPScgKyBlbmNvZGVVUklDb21wb25lbnQoc29zbCk7XG4gICAgcmV0dXJuIHRoaXMucmVxdWVzdDxTZWFyY2hSZXN1bHQ+KHVybCk7XG4gIH1cblxuICAvKipcbiAgICpcbiAgICovXG4gIHF1ZXJ5TW9yZShsb2NhdG9yOiBzdHJpbmcsIG9wdGlvbnM/OiBRdWVyeU9wdGlvbnMpIHtcbiAgICByZXR1cm4gbmV3IFF1ZXJ5PFMsIFNPYmplY3ROYW1lczxTPiwgUmVjb3JkLCAnUXVlcnlSZXN1bHQnPihcbiAgICAgIHRoaXMsXG4gICAgICB7IGxvY2F0b3IgfSxcbiAgICAgIG9wdGlvbnMsXG4gICAgKTtcbiAgfVxuXG4gIC8qICovXG4gIF9lbnN1cmVWZXJzaW9uKG1ham9yVmVyc2lvbjogbnVtYmVyKSB7XG4gICAgY29uc3QgdmVyc2lvbnMgPSB0aGlzLnZlcnNpb24uc3BsaXQoJy4nKTtcbiAgICByZXR1cm4gcGFyc2VJbnQodmVyc2lvbnNbMF0sIDEwKSA+PSBtYWpvclZlcnNpb247XG4gIH1cblxuICAvKiAqL1xuICBfc3VwcG9ydHMoZmVhdHVyZTogc3RyaW5nKSB7XG4gICAgc3dpdGNoIChmZWF0dXJlKSB7XG4gICAgICBjYXNlICdzb2JqZWN0LWNvbGxlY3Rpb24nOiAvLyBzb2JqZWN0IGNvbGxlY3Rpb24gaXMgYXZhaWxhYmxlIG9ubHkgaW4gQVBJIHZlciA0Mi4wK1xuICAgICAgICByZXR1cm4gdGhpcy5fZW5zdXJlVmVyc2lvbig0Mik7XG4gICAgICBkZWZhdWx0OlxuICAgICAgICByZXR1cm4gZmFsc2U7XG4gICAgfVxuICB9XG5cbiAgLyoqXG4gICAqIFJldHJpZXZlIHNwZWNpZmllZCByZWNvcmRzXG4gICAqL1xuICByZXRyaWV2ZTxOIGV4dGVuZHMgU09iamVjdE5hbWVzPFM+PihcbiAgICB0eXBlOiBOLFxuICAgIGlkczogc3RyaW5nLFxuICAgIG9wdGlvbnM/OiBSZXRyaWV2ZU9wdGlvbnMsXG4gICk6IFByb21pc2U8UmVjb3JkPjtcbiAgcmV0cmlldmU8TiBleHRlbmRzIFNPYmplY3ROYW1lczxTPj4oXG4gICAgdHlwZTogTixcbiAgICBpZHM6IHN0cmluZ1tdLFxuICAgIG9wdGlvbnM/OiBSZXRyaWV2ZU9wdGlvbnMsXG4gICk6IFByb21pc2U8UmVjb3JkW10+O1xuICByZXRyaWV2ZTxOIGV4dGVuZHMgU09iamVjdE5hbWVzPFM+PihcbiAgICB0eXBlOiBOLFxuICAgIGlkczogc3RyaW5nIHwgc3RyaW5nW10sXG4gICAgb3B0aW9ucz86IFJldHJpZXZlT3B0aW9ucyxcbiAgKTogUHJvbWlzZTxSZWNvcmQgfCBSZWNvcmRbXT47XG4gIGFzeW5jIHJldHJpZXZlKFxuICAgIHR5cGU6IHN0cmluZyxcbiAgICBpZHM6IHN0cmluZyB8IHN0cmluZ1tdLFxuICAgIG9wdGlvbnM6IFJldHJpZXZlT3B0aW9ucyA9IHt9LFxuICApIHtcbiAgICByZXR1cm4gQXJyYXkuaXNBcnJheShpZHMpXG4gICAgICA/IC8vIGNoZWNrIHRoZSB2ZXJzaW9uIHdoZXRoZXIgU09iamVjdCBjb2xsZWN0aW9uIEFQSSBpcyBzdXBwb3J0ZWQgKDQyLjApXG4gICAgICAgIHRoaXMuX2Vuc3VyZVZlcnNpb24oNDIpXG4gICAgICAgID8gdGhpcy5fcmV0cmlldmVNYW55KHR5cGUsIGlkcywgb3B0aW9ucylcbiAgICAgICAgOiB0aGlzLl9yZXRyaWV2ZVBhcmFsbGVsKHR5cGUsIGlkcywgb3B0aW9ucylcbiAgICAgIDogdGhpcy5fcmV0cmlldmVTaW5nbGUodHlwZSwgaWRzLCBvcHRpb25zKTtcbiAgfVxuXG4gIC8qKiBAcHJpdmF0ZSAqL1xuICBhc3luYyBfcmV0cmlldmVTaW5nbGUodHlwZTogc3RyaW5nLCBpZDogc3RyaW5nLCBvcHRpb25zOiBSZXRyaWV2ZU9wdGlvbnMpIHtcbiAgICBpZiAoIWlkKSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoJ0ludmFsaWQgcmVjb3JkIElELiBTcGVjaWZ5IHZhbGlkIHJlY29yZCBJRCB2YWx1ZScpO1xuICAgIH1cbiAgICBsZXQgdXJsID0gW3RoaXMuX2Jhc2VVcmwoKSwgJ3NvYmplY3RzJywgdHlwZSwgaWRdLmpvaW4oJy8nKTtcbiAgICBjb25zdCB7IGZpZWxkcywgaGVhZGVycyB9ID0gb3B0aW9ucztcbiAgICBpZiAoZmllbGRzKSB7XG4gICAgICB1cmwgKz0gYD9maWVsZHM9JHtmaWVsZHMuam9pbignLCcpfWA7XG4gICAgfVxuICAgIHJldHVybiB0aGlzLnJlcXVlc3QoeyBtZXRob2Q6ICdHRVQnLCB1cmwsIGhlYWRlcnMgfSk7XG4gIH1cblxuICAvKiogQHByaXZhdGUgKi9cbiAgYXN5bmMgX3JldHJpZXZlUGFyYWxsZWwoXG4gICAgdHlwZTogc3RyaW5nLFxuICAgIGlkczogc3RyaW5nW10sXG4gICAgb3B0aW9uczogUmV0cmlldmVPcHRpb25zLFxuICApIHtcbiAgICBpZiAoaWRzLmxlbmd0aCA+IHRoaXMuX21heFJlcXVlc3QpIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcignRXhjZWVkZWQgbWF4IGxpbWl0IG9mIGNvbmN1cnJlbnQgY2FsbCcpO1xuICAgIH1cbiAgICByZXR1cm4gUHJvbWlzZS5hbGwoXG4gICAgICBpZHMubWFwKChpZCkgPT5cbiAgICAgICAgdGhpcy5fcmV0cmlldmVTaW5nbGUodHlwZSwgaWQsIG9wdGlvbnMpLmNhdGNoKChlcnIpID0+IHtcbiAgICAgICAgICBpZiAob3B0aW9ucy5hbGxPck5vbmUgfHwgZXJyLmVycm9yQ29kZSAhPT0gJ05PVF9GT1VORCcpIHtcbiAgICAgICAgICAgIHRocm93IGVycjtcbiAgICAgICAgICB9XG4gICAgICAgICAgcmV0dXJuIG51bGw7XG4gICAgICAgIH0pLFxuICAgICAgKSxcbiAgICApO1xuICB9XG5cbiAgLyoqIEBwcml2YXRlICovXG4gIGFzeW5jIF9yZXRyaWV2ZU1hbnkodHlwZTogc3RyaW5nLCBpZHM6IHN0cmluZ1tdLCBvcHRpb25zOiBSZXRyaWV2ZU9wdGlvbnMpIHtcbiAgICBpZiAoaWRzLmxlbmd0aCA9PT0gMCkge1xuICAgICAgcmV0dXJuIFtdO1xuICAgIH1cbiAgICBjb25zdCB1cmwgPSBbdGhpcy5fYmFzZVVybCgpLCAnY29tcG9zaXRlJywgJ3NvYmplY3RzJywgdHlwZV0uam9pbignLycpO1xuICAgIGNvbnN0IGZpZWxkcyA9XG4gICAgICBvcHRpb25zLmZpZWxkcyB8fFxuICAgICAgKGF3YWl0IHRoaXMuZGVzY3JpYmUkKHR5cGUpKS5maWVsZHMubWFwKChmaWVsZCkgPT4gZmllbGQubmFtZSk7XG4gICAgcmV0dXJuIHRoaXMucmVxdWVzdCh7XG4gICAgICBtZXRob2Q6ICdQT1NUJyxcbiAgICAgIHVybCxcbiAgICAgIGJvZHk6IEpTT04uc3RyaW5naWZ5KHsgaWRzLCBmaWVsZHMgfSksXG4gICAgICBoZWFkZXJzOiB7XG4gICAgICAgIC4uLihvcHRpb25zLmhlYWRlcnMgfHwge30pLFxuICAgICAgICAnY29udGVudC10eXBlJzogJ2FwcGxpY2F0aW9uL2pzb24nLFxuICAgICAgfSxcbiAgICB9KTtcbiAgfVxuXG4gIC8qKlxuICAgKiBDcmVhdGUgcmVjb3Jkc1xuICAgKi9cbiAgY3JlYXRlPFxuICAgIE4gZXh0ZW5kcyBTT2JqZWN0TmFtZXM8Uz4sXG4gICAgSW5wdXRSZWNvcmQgZXh0ZW5kcyBTT2JqZWN0SW5wdXRSZWNvcmQ8UywgTj4gPSBTT2JqZWN0SW5wdXRSZWNvcmQ8UywgTj5cbiAgPihcbiAgICB0eXBlOiBOLFxuICAgIHJlY29yZHM6IElucHV0UmVjb3JkW10sXG4gICAgb3B0aW9ucz86IERtbE9wdGlvbnMsXG4gICk6IFByb21pc2U8U2F2ZVJlc3VsdFtdPjtcbiAgY3JlYXRlPFxuICAgIE4gZXh0ZW5kcyBTT2JqZWN0TmFtZXM8Uz4sXG4gICAgSW5wdXRSZWNvcmQgZXh0ZW5kcyBTT2JqZWN0SW5wdXRSZWNvcmQ8UywgTj4gPSBTT2JqZWN0SW5wdXRSZWNvcmQ8UywgTj5cbiAgPih0eXBlOiBOLCByZWNvcmQ6IElucHV0UmVjb3JkLCBvcHRpb25zPzogRG1sT3B0aW9ucyk6IFByb21pc2U8U2F2ZVJlc3VsdD47XG4gIGNyZWF0ZTxcbiAgICBOIGV4dGVuZHMgU09iamVjdE5hbWVzPFM+LFxuICAgIElucHV0UmVjb3JkIGV4dGVuZHMgU09iamVjdElucHV0UmVjb3JkPFMsIE4+ID0gU09iamVjdElucHV0UmVjb3JkPFMsIE4+XG4gID4oXG4gICAgdHlwZTogTixcbiAgICByZWNvcmRzOiBJbnB1dFJlY29yZCB8IElucHV0UmVjb3JkW10sXG4gICAgb3B0aW9ucz86IERtbE9wdGlvbnMsXG4gICk6IFByb21pc2U8U2F2ZVJlc3VsdCB8IFNhdmVSZXN1bHRbXT47XG4gIC8qKlxuICAgKiBAcGFyYW0gdHlwZVxuICAgKiBAcGFyYW0gcmVjb3Jkc1xuICAgKiBAcGFyYW0gb3B0aW9uc1xuICAgKi9cbiAgYXN5bmMgY3JlYXRlKFxuICAgIHR5cGU6IHN0cmluZyxcbiAgICByZWNvcmRzOiBSZWNvcmQgfCBSZWNvcmRbXSxcbiAgICBvcHRpb25zOiBEbWxPcHRpb25zID0ge30sXG4gICkge1xuICAgIGNvbnN0IHJldCA9IEFycmF5LmlzQXJyYXkocmVjb3JkcylcbiAgICAgID8gLy8gY2hlY2sgdGhlIHZlcnNpb24gd2hldGhlciBTT2JqZWN0IGNvbGxlY3Rpb24gQVBJIGlzIHN1cHBvcnRlZCAoNDIuMClcbiAgICAgICAgdGhpcy5fZW5zdXJlVmVyc2lvbig0MilcbiAgICAgICAgPyBhd2FpdCB0aGlzLl9jcmVhdGVNYW55KHR5cGUsIHJlY29yZHMsIG9wdGlvbnMpXG4gICAgICAgIDogYXdhaXQgdGhpcy5fY3JlYXRlUGFyYWxsZWwodHlwZSwgcmVjb3Jkcywgb3B0aW9ucylcbiAgICAgIDogYXdhaXQgdGhpcy5fY3JlYXRlU2luZ2xlKHR5cGUsIHJlY29yZHMsIG9wdGlvbnMpO1xuICAgIHJldHVybiByZXQ7XG4gIH1cblxuICAvKiogQHByaXZhdGUgKi9cbiAgYXN5bmMgX2NyZWF0ZVNpbmdsZSh0eXBlOiBzdHJpbmcsIHJlY29yZDogUmVjb3JkLCBvcHRpb25zOiBEbWxPcHRpb25zKSB7XG4gICAgY29uc3QgeyBJZCwgdHlwZTogcnR5cGUsIGF0dHJpYnV0ZXMsIC4uLnJlYyB9ID0gcmVjb3JkO1xuICAgIGNvbnN0IHNvYmplY3RUeXBlID0gdHlwZSB8fCBhdHRyaWJ1dGVzPy50eXBlIHx8IHJ0eXBlO1xuICAgIGlmICghc29iamVjdFR5cGUpIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcignTm8gU09iamVjdCBUeXBlIGRlZmluZWQgaW4gcmVjb3JkJyk7XG4gICAgfVxuICAgIGNvbnN0IHVybCA9IFt0aGlzLl9iYXNlVXJsKCksICdzb2JqZWN0cycsIHNvYmplY3RUeXBlXS5qb2luKCcvJyk7XG4gICAgbGV0IGNvbnRlbnRUeXBlLCBib2R5O1xuXG4gICAgaWYgKG9wdGlvbnM/Lm11bHRpcGFydEZpbGVGaWVsZHMpIHtcbiAgICAgIC8vIFNlbmQgdGhlIHJlY29yZCBhcyBhIG11bHRpcGFydC9mb3JtLWRhdGEgcmVxdWVzdC4gVXNlZnVsIGZvciBmaWVsZHMgY29udGFpbmluZyBsYXJnZSBiaW5hcnkgYmxvYnMuXG4gICAgICBjb25zdCBmb3JtID0gbmV3IEZvcm1EYXRhKCk7XG4gICAgICAvLyBFeHRyYWN0IHRoZSBmaWVsZHMgcmVxdWVzdGVkIHRvIGJlIHNlbnQgc2VwYXJhdGVseSBmcm9tIHRoZSBKU09OXG4gICAgICBPYmplY3QuZW50cmllcyhvcHRpb25zLm11bHRpcGFydEZpbGVGaWVsZHMpLmZvckVhY2goXG4gICAgICAgIChbZmllbGROYW1lLCBmaWxlRGV0YWlsc10pID0+IHtcbiAgICAgICAgICBmb3JtLmFwcGVuZChcbiAgICAgICAgICAgIGZpZWxkTmFtZSxcbiAgICAgICAgICAgIEJ1ZmZlci5mcm9tKHJlY1tmaWVsZE5hbWVdLCAnYmFzZTY0JyksXG4gICAgICAgICAgICBmaWxlRGV0YWlscyxcbiAgICAgICAgICApO1xuICAgICAgICAgIGRlbGV0ZSByZWNbZmllbGROYW1lXTtcbiAgICAgICAgfSxcbiAgICAgICk7XG4gICAgICAvLyBTZXJpYWxpemUgdGhlIHJlbWFpbmluZyBmaWVsZHMgYXMgSlNPTlxuICAgICAgZm9ybS5hcHBlbmQodHlwZSwgSlNPTi5zdHJpbmdpZnkocmVjKSwge1xuICAgICAgICBjb250ZW50VHlwZTogJ2FwcGxpY2F0aW9uL2pzb24nLFxuICAgICAgfSk7XG4gICAgICBjb250ZW50VHlwZSA9IGZvcm0uZ2V0SGVhZGVycygpWydjb250ZW50LXR5cGUnXTsgLy8gVGhpcyBpcyBuZWNlc3NhcnkgdG8gZW5zdXJlIHRoZSAnYm91bmRhcnknIGlzIHByZXNlbnRcbiAgICAgIGJvZHkgPSBmb3JtO1xuICAgIH0gZWxzZSB7XG4gICAgICAvLyBEZWZhdWx0IGJlaGF2aW9yOiBzZW5kIHRoZSByZXF1ZXN0IGFzIEpTT05cbiAgICAgIGNvbnRlbnRUeXBlID0gJ2FwcGxpY2F0aW9uL2pzb24nO1xuICAgICAgYm9keSA9IEpTT04uc3RyaW5naWZ5KHJlYyk7XG4gICAgfVxuXG4gICAgcmV0dXJuIHRoaXMucmVxdWVzdCh7XG4gICAgICBtZXRob2Q6ICdQT1NUJyxcbiAgICAgIHVybCxcbiAgICAgIGJvZHk6IGJvZHksXG4gICAgICBoZWFkZXJzOiB7XG4gICAgICAgIC4uLihvcHRpb25zLmhlYWRlcnMgfHwge30pLFxuICAgICAgICAnY29udGVudC10eXBlJzogY29udGVudFR5cGUsXG4gICAgICB9LFxuICAgIH0pO1xuICB9XG5cbiAgLyoqIEBwcml2YXRlICovXG4gIGFzeW5jIF9jcmVhdGVQYXJhbGxlbCh0eXBlOiBzdHJpbmcsIHJlY29yZHM6IFJlY29yZFtdLCBvcHRpb25zOiBEbWxPcHRpb25zKSB7XG4gICAgaWYgKHJlY29yZHMubGVuZ3RoID4gdGhpcy5fbWF4UmVxdWVzdCkge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKCdFeGNlZWRlZCBtYXggbGltaXQgb2YgY29uY3VycmVudCBjYWxsJyk7XG4gICAgfVxuICAgIHJldHVybiBQcm9taXNlLmFsbChcbiAgICAgIHJlY29yZHMubWFwKChyZWNvcmQpID0+XG4gICAgICAgIHRoaXMuX2NyZWF0ZVNpbmdsZSh0eXBlLCByZWNvcmQsIG9wdGlvbnMpLmNhdGNoKChlcnIpID0+IHtcbiAgICAgICAgICAvLyBiZSBhd2FyZSB0aGF0IGFsbE9yTm9uZSBpbiBwYXJhbGxlbCBtb2RlIHdpbGwgbm90IHJldmVydCB0aGUgb3RoZXIgc3VjY2Vzc2Z1bCByZXF1ZXN0c1xuICAgICAgICAgIC8vIGl0IG9ubHkgcmFpc2VzIGVycm9yIHdoZW4gbWV0IGF0IGxlYXN0IG9uZSBmYWlsZWQgcmVxdWVzdC5cbiAgICAgICAgICBpZiAob3B0aW9ucy5hbGxPck5vbmUgfHwgIWVyci5lcnJvckNvZGUpIHtcbiAgICAgICAgICAgIHRocm93IGVycjtcbiAgICAgICAgICB9XG4gICAgICAgICAgcmV0dXJuIHRvU2F2ZVJlc3VsdChlcnIpO1xuICAgICAgICB9KSxcbiAgICAgICksXG4gICAgKTtcbiAgfVxuXG4gIC8qKiBAcHJpdmF0ZSAqL1xuICBhc3luYyBfY3JlYXRlTWFueShcbiAgICB0eXBlOiBzdHJpbmcsXG4gICAgcmVjb3JkczogUmVjb3JkW10sXG4gICAgb3B0aW9uczogRG1sT3B0aW9ucyxcbiAgKTogUHJvbWlzZTxTYXZlUmVzdWx0W10+IHtcbiAgICBpZiAocmVjb3Jkcy5sZW5ndGggPT09IDApIHtcbiAgICAgIHJldHVybiBQcm9taXNlLnJlc29sdmUoW10pO1xuICAgIH1cbiAgICBpZiAocmVjb3Jkcy5sZW5ndGggPiBNQVhfRE1MX0NPVU5UICYmIG9wdGlvbnMuYWxsb3dSZWN1cnNpdmUpIHtcbiAgICAgIHJldHVybiBbXG4gICAgICAgIC4uLihhd2FpdCB0aGlzLl9jcmVhdGVNYW55KFxuICAgICAgICAgIHR5cGUsXG4gICAgICAgICAgcmVjb3Jkcy5zbGljZSgwLCBNQVhfRE1MX0NPVU5UKSxcbiAgICAgICAgICBvcHRpb25zLFxuICAgICAgICApKSxcbiAgICAgICAgLi4uKGF3YWl0IHRoaXMuX2NyZWF0ZU1hbnkoXG4gICAgICAgICAgdHlwZSxcbiAgICAgICAgICByZWNvcmRzLnNsaWNlKE1BWF9ETUxfQ09VTlQpLFxuICAgICAgICAgIG9wdGlvbnMsXG4gICAgICAgICkpLFxuICAgICAgXTtcbiAgICB9XG4gICAgY29uc3QgX3JlY29yZHMgPSByZWNvcmRzLm1hcCgocmVjb3JkKSA9PiB7XG4gICAgICBjb25zdCB7IElkLCB0eXBlOiBydHlwZSwgYXR0cmlidXRlcywgLi4ucmVjIH0gPSByZWNvcmQ7XG4gICAgICBjb25zdCBzb2JqZWN0VHlwZSA9IHR5cGUgfHwgYXR0cmlidXRlcz8udHlwZSB8fCBydHlwZTtcbiAgICAgIGlmICghc29iamVjdFR5cGUpIHtcbiAgICAgICAgdGhyb3cgbmV3IEVycm9yKCdObyBTT2JqZWN0IFR5cGUgZGVmaW5lZCBpbiByZWNvcmQnKTtcbiAgICAgIH1cbiAgICAgIHJldHVybiB7IGF0dHJpYnV0ZXM6IHsgdHlwZTogc29iamVjdFR5cGUgfSwgLi4ucmVjIH07XG4gICAgfSk7XG4gICAgY29uc3QgdXJsID0gW3RoaXMuX2Jhc2VVcmwoKSwgJ2NvbXBvc2l0ZScsICdzb2JqZWN0cyddLmpvaW4oJy8nKTtcbiAgICByZXR1cm4gdGhpcy5yZXF1ZXN0KHtcbiAgICAgIG1ldGhvZDogJ1BPU1QnLFxuICAgICAgdXJsLFxuICAgICAgYm9keTogSlNPTi5zdHJpbmdpZnkoe1xuICAgICAgICBhbGxPck5vbmU6IG9wdGlvbnMuYWxsT3JOb25lIHx8IGZhbHNlLFxuICAgICAgICByZWNvcmRzOiBfcmVjb3JkcyxcbiAgICAgIH0pLFxuICAgICAgaGVhZGVyczoge1xuICAgICAgICAuLi4ob3B0aW9ucy5oZWFkZXJzIHx8IHt9KSxcbiAgICAgICAgJ2NvbnRlbnQtdHlwZSc6ICdhcHBsaWNhdGlvbi9qc29uJyxcbiAgICAgIH0sXG4gICAgfSk7XG4gIH1cblxuICAvKipcbiAgICogU3lub255bSBvZiBDb25uZWN0aW9uI2NyZWF0ZSgpXG4gICAqL1xuICBpbnNlcnQgPSB0aGlzLmNyZWF0ZTtcblxuICAvKipcbiAgICogVXBkYXRlIHJlY29yZHNcbiAgICovXG4gIHVwZGF0ZTxcbiAgICBOIGV4dGVuZHMgU09iamVjdE5hbWVzPFM+LFxuICAgIFVwZGF0ZVJlY29yZCBleHRlbmRzIFNPYmplY3RVcGRhdGVSZWNvcmQ8UywgTj4gPSBTT2JqZWN0VXBkYXRlUmVjb3JkPFMsIE4+XG4gID4oXG4gICAgdHlwZTogTixcbiAgICByZWNvcmRzOiBVcGRhdGVSZWNvcmRbXSxcbiAgICBvcHRpb25zPzogRG1sT3B0aW9ucyxcbiAgKTogUHJvbWlzZTxTYXZlUmVzdWx0W10+O1xuICB1cGRhdGU8XG4gICAgTiBleHRlbmRzIFNPYmplY3ROYW1lczxTPixcbiAgICBVcGRhdGVSZWNvcmQgZXh0ZW5kcyBTT2JqZWN0VXBkYXRlUmVjb3JkPFMsIE4+ID0gU09iamVjdFVwZGF0ZVJlY29yZDxTLCBOPlxuICA+KHR5cGU6IE4sIHJlY29yZDogVXBkYXRlUmVjb3JkLCBvcHRpb25zPzogRG1sT3B0aW9ucyk6IFByb21pc2U8U2F2ZVJlc3VsdD47XG4gIHVwZGF0ZTxcbiAgICBOIGV4dGVuZHMgU09iamVjdE5hbWVzPFM+LFxuICAgIFVwZGF0ZVJlY29yZCBleHRlbmRzIFNPYmplY3RVcGRhdGVSZWNvcmQ8UywgTj4gPSBTT2JqZWN0VXBkYXRlUmVjb3JkPFMsIE4+XG4gID4oXG4gICAgdHlwZTogTixcbiAgICByZWNvcmRzOiBVcGRhdGVSZWNvcmQgfCBVcGRhdGVSZWNvcmRbXSxcbiAgICBvcHRpb25zPzogRG1sT3B0aW9ucyxcbiAgKTogUHJvbWlzZTxTYXZlUmVzdWx0IHwgU2F2ZVJlc3VsdFtdPjtcbiAgLyoqXG4gICAqIEBwYXJhbSB0eXBlXG4gICAqIEBwYXJhbSByZWNvcmRzXG4gICAqIEBwYXJhbSBvcHRpb25zXG4gICAqL1xuICB1cGRhdGU8TiBleHRlbmRzIFNPYmplY3ROYW1lczxTPj4oXG4gICAgdHlwZTogTixcbiAgICByZWNvcmRzOiBSZWNvcmQgfCBSZWNvcmRbXSxcbiAgICBvcHRpb25zOiBEbWxPcHRpb25zID0ge30sXG4gICk6IFByb21pc2U8U2F2ZVJlc3VsdCB8IFNhdmVSZXN1bHRbXT4ge1xuICAgIHJldHVybiBBcnJheS5pc0FycmF5KHJlY29yZHMpXG4gICAgICA/IC8vIGNoZWNrIHRoZSB2ZXJzaW9uIHdoZXRoZXIgU09iamVjdCBjb2xsZWN0aW9uIEFQSSBpcyBzdXBwb3J0ZWQgKDQyLjApXG4gICAgICAgIHRoaXMuX2Vuc3VyZVZlcnNpb24oNDIpXG4gICAgICAgID8gdGhpcy5fdXBkYXRlTWFueSh0eXBlLCByZWNvcmRzLCBvcHRpb25zKVxuICAgICAgICA6IHRoaXMuX3VwZGF0ZVBhcmFsbGVsKHR5cGUsIHJlY29yZHMsIG9wdGlvbnMpXG4gICAgICA6IHRoaXMuX3VwZGF0ZVNpbmdsZSh0eXBlLCByZWNvcmRzLCBvcHRpb25zKTtcbiAgfVxuXG4gIC8qKiBAcHJpdmF0ZSAqL1xuICBhc3luYyBfdXBkYXRlU2luZ2xlKFxuICAgIHR5cGU6IHN0cmluZyxcbiAgICByZWNvcmQ6IFJlY29yZCxcbiAgICBvcHRpb25zOiBEbWxPcHRpb25zLFxuICApOiBQcm9taXNlPFNhdmVSZXN1bHQ+IHtcbiAgICBjb25zdCB7IElkOiBpZCwgdHlwZTogcnR5cGUsIGF0dHJpYnV0ZXMsIC4uLnJlYyB9ID0gcmVjb3JkO1xuICAgIGlmICghaWQpIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcignUmVjb3JkIGlkIGlzIG5vdCBmb3VuZCBpbiByZWNvcmQuJyk7XG4gICAgfVxuICAgIGNvbnN0IHNvYmplY3RUeXBlID0gdHlwZSB8fCBhdHRyaWJ1dGVzPy50eXBlIHx8IHJ0eXBlO1xuICAgIGlmICghc29iamVjdFR5cGUpIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcignTm8gU09iamVjdCBUeXBlIGRlZmluZWQgaW4gcmVjb3JkJyk7XG4gICAgfVxuICAgIGNvbnN0IHVybCA9IFt0aGlzLl9iYXNlVXJsKCksICdzb2JqZWN0cycsIHNvYmplY3RUeXBlLCBpZF0uam9pbignLycpO1xuICAgIHJldHVybiB0aGlzLnJlcXVlc3QoXG4gICAgICB7XG4gICAgICAgIG1ldGhvZDogJ1BBVENIJyxcbiAgICAgICAgdXJsLFxuICAgICAgICBib2R5OiBKU09OLnN0cmluZ2lmeShyZWMpLFxuICAgICAgICBoZWFkZXJzOiB7XG4gICAgICAgICAgLi4uKG9wdGlvbnMuaGVhZGVycyB8fCB7fSksXG4gICAgICAgICAgJ2NvbnRlbnQtdHlwZSc6ICdhcHBsaWNhdGlvbi9qc29uJyxcbiAgICAgICAgfSxcbiAgICAgIH0sXG4gICAgICB7XG4gICAgICAgIG5vQ29udGVudFJlc3BvbnNlOiB7IGlkLCBzdWNjZXNzOiB0cnVlLCBlcnJvcnM6IFtdIH0sXG4gICAgICB9LFxuICAgICk7XG4gIH1cblxuICAvKiogQHByaXZhdGUgKi9cbiAgYXN5bmMgX3VwZGF0ZVBhcmFsbGVsKHR5cGU6IHN0cmluZywgcmVjb3JkczogUmVjb3JkW10sIG9wdGlvbnM6IERtbE9wdGlvbnMpIHtcbiAgICBpZiAocmVjb3Jkcy5sZW5ndGggPiB0aGlzLl9tYXhSZXF1ZXN0KSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoJ0V4Y2VlZGVkIG1heCBsaW1pdCBvZiBjb25jdXJyZW50IGNhbGwnKTtcbiAgICB9XG4gICAgcmV0dXJuIFByb21pc2UuYWxsKFxuICAgICAgcmVjb3Jkcy5tYXAoKHJlY29yZCkgPT5cbiAgICAgICAgdGhpcy5fdXBkYXRlU2luZ2xlKHR5cGUsIHJlY29yZCwgb3B0aW9ucykuY2F0Y2goKGVycikgPT4ge1xuICAgICAgICAgIC8vIGJlIGF3YXJlIHRoYXQgYWxsT3JOb25lIGluIHBhcmFsbGVsIG1vZGUgd2lsbCBub3QgcmV2ZXJ0IHRoZSBvdGhlciBzdWNjZXNzZnVsIHJlcXVlc3RzXG4gICAgICAgICAgLy8gaXQgb25seSByYWlzZXMgZXJyb3Igd2hlbiBtZXQgYXQgbGVhc3Qgb25lIGZhaWxlZCByZXF1ZXN0LlxuICAgICAgICAgIGlmIChvcHRpb25zLmFsbE9yTm9uZSB8fCAhZXJyLmVycm9yQ29kZSkge1xuICAgICAgICAgICAgdGhyb3cgZXJyO1xuICAgICAgICAgIH1cbiAgICAgICAgICByZXR1cm4gdG9TYXZlUmVzdWx0KGVycik7XG4gICAgICAgIH0pLFxuICAgICAgKSxcbiAgICApO1xuICB9XG5cbiAgLyoqIEBwcml2YXRlICovXG4gIGFzeW5jIF91cGRhdGVNYW55KFxuICAgIHR5cGU6IHN0cmluZyxcbiAgICByZWNvcmRzOiBSZWNvcmRbXSxcbiAgICBvcHRpb25zOiBEbWxPcHRpb25zLFxuICApOiBQcm9taXNlPFNhdmVSZXN1bHRbXT4ge1xuICAgIGlmIChyZWNvcmRzLmxlbmd0aCA9PT0gMCkge1xuICAgICAgcmV0dXJuIFtdO1xuICAgIH1cbiAgICBpZiAocmVjb3Jkcy5sZW5ndGggPiBNQVhfRE1MX0NPVU5UICYmIG9wdGlvbnMuYWxsb3dSZWN1cnNpdmUpIHtcbiAgICAgIHJldHVybiBbXG4gICAgICAgIC4uLihhd2FpdCB0aGlzLl91cGRhdGVNYW55KFxuICAgICAgICAgIHR5cGUsXG4gICAgICAgICAgcmVjb3Jkcy5zbGljZSgwLCBNQVhfRE1MX0NPVU5UKSxcbiAgICAgICAgICBvcHRpb25zLFxuICAgICAgICApKSxcbiAgICAgICAgLi4uKGF3YWl0IHRoaXMuX3VwZGF0ZU1hbnkoXG4gICAgICAgICAgdHlwZSxcbiAgICAgICAgICByZWNvcmRzLnNsaWNlKE1BWF9ETUxfQ09VTlQpLFxuICAgICAgICAgIG9wdGlvbnMsXG4gICAgICAgICkpLFxuICAgICAgXTtcbiAgICB9XG4gICAgY29uc3QgX3JlY29yZHMgPSByZWNvcmRzLm1hcCgocmVjb3JkKSA9PiB7XG4gICAgICBjb25zdCB7IElkOiBpZCwgdHlwZTogcnR5cGUsIGF0dHJpYnV0ZXMsIC4uLnJlYyB9ID0gcmVjb3JkO1xuICAgICAgaWYgKCFpZCkge1xuICAgICAgICB0aHJvdyBuZXcgRXJyb3IoJ1JlY29yZCBpZCBpcyBub3QgZm91bmQgaW4gcmVjb3JkLicpO1xuICAgICAgfVxuICAgICAgY29uc3Qgc29iamVjdFR5cGUgPSB0eXBlIHx8IGF0dHJpYnV0ZXM/LnR5cGUgfHwgcnR5cGU7XG4gICAgICBpZiAoIXNvYmplY3RUeXBlKSB7XG4gICAgICAgIHRocm93IG5ldyBFcnJvcignTm8gU09iamVjdCBUeXBlIGRlZmluZWQgaW4gcmVjb3JkJyk7XG4gICAgICB9XG4gICAgICByZXR1cm4geyBpZCwgYXR0cmlidXRlczogeyB0eXBlOiBzb2JqZWN0VHlwZSB9LCAuLi5yZWMgfTtcbiAgICB9KTtcbiAgICBjb25zdCB1cmwgPSBbdGhpcy5fYmFzZVVybCgpLCAnY29tcG9zaXRlJywgJ3NvYmplY3RzJ10uam9pbignLycpO1xuICAgIHJldHVybiB0aGlzLnJlcXVlc3Qoe1xuICAgICAgbWV0aG9kOiAnUEFUQ0gnLFxuICAgICAgdXJsLFxuICAgICAgYm9keTogSlNPTi5zdHJpbmdpZnkoe1xuICAgICAgICBhbGxPck5vbmU6IG9wdGlvbnMuYWxsT3JOb25lIHx8IGZhbHNlLFxuICAgICAgICByZWNvcmRzOiBfcmVjb3JkcyxcbiAgICAgIH0pLFxuICAgICAgaGVhZGVyczoge1xuICAgICAgICAuLi4ob3B0aW9ucy5oZWFkZXJzIHx8IHt9KSxcbiAgICAgICAgJ2NvbnRlbnQtdHlwZSc6ICdhcHBsaWNhdGlvbi9qc29uJyxcbiAgICAgIH0sXG4gICAgfSk7XG4gIH1cblxuICAvKipcbiAgICogVXBzZXJ0IHJlY29yZHNcbiAgICovXG4gIHVwc2VydDxcbiAgICBOIGV4dGVuZHMgU09iamVjdE5hbWVzPFM+LFxuICAgIElucHV0UmVjb3JkIGV4dGVuZHMgU09iamVjdElucHV0UmVjb3JkPFMsIE4+ID0gU09iamVjdElucHV0UmVjb3JkPFMsIE4+LFxuICAgIEZpZWxkTmFtZXMgZXh0ZW5kcyBTT2JqZWN0RmllbGROYW1lczxTLCBOPiA9IFNPYmplY3RGaWVsZE5hbWVzPFMsIE4+XG4gID4oXG4gICAgdHlwZTogTixcbiAgICByZWNvcmRzOiBJbnB1dFJlY29yZFtdLFxuICAgIGV4dElkRmllbGQ6IEZpZWxkTmFtZXMsXG4gICAgb3B0aW9ucz86IERtbE9wdGlvbnMsXG4gICk6IFByb21pc2U8VXBzZXJ0UmVzdWx0W10+O1xuICB1cHNlcnQ8XG4gICAgTiBleHRlbmRzIFNPYmplY3ROYW1lczxTPixcbiAgICBJbnB1dFJlY29yZCBleHRlbmRzIFNPYmplY3RJbnB1dFJlY29yZDxTLCBOPiA9IFNPYmplY3RJbnB1dFJlY29yZDxTLCBOPixcbiAgICBGaWVsZE5hbWVzIGV4dGVuZHMgU09iamVjdEZpZWxkTmFtZXM8UywgTj4gPSBTT2JqZWN0RmllbGROYW1lczxTLCBOPlxuICA+KFxuICAgIHR5cGU6IE4sXG4gICAgcmVjb3JkOiBJbnB1dFJlY29yZCxcbiAgICBleHRJZEZpZWxkOiBGaWVsZE5hbWVzLFxuICAgIG9wdGlvbnM/OiBEbWxPcHRpb25zLFxuICApOiBQcm9taXNlPFVwc2VydFJlc3VsdD47XG4gIHVwc2VydDxcbiAgICBOIGV4dGVuZHMgU09iamVjdE5hbWVzPFM+LFxuICAgIElucHV0UmVjb3JkIGV4dGVuZHMgU09iamVjdElucHV0UmVjb3JkPFMsIE4+ID0gU09iamVjdElucHV0UmVjb3JkPFMsIE4+LFxuICAgIEZpZWxkTmFtZXMgZXh0ZW5kcyBTT2JqZWN0RmllbGROYW1lczxTLCBOPiA9IFNPYmplY3RGaWVsZE5hbWVzPFMsIE4+XG4gID4oXG4gICAgdHlwZTogTixcbiAgICByZWNvcmRzOiBJbnB1dFJlY29yZCB8IElucHV0UmVjb3JkW10sXG4gICAgZXh0SWRGaWVsZDogRmllbGROYW1lcyxcbiAgICBvcHRpb25zPzogRG1sT3B0aW9ucyxcbiAgKTogUHJvbWlzZTxVcHNlcnRSZXN1bHQgfCBVcHNlcnRSZXN1bHRbXT47XG4gIC8qKlxuICAgKlxuICAgKiBAcGFyYW0gdHlwZVxuICAgKiBAcGFyYW0gcmVjb3Jkc1xuICAgKiBAcGFyYW0gZXh0SWRGaWVsZFxuICAgKiBAcGFyYW0gb3B0aW9uc1xuICAgKi9cbiAgYXN5bmMgdXBzZXJ0KFxuICAgIHR5cGU6IHN0cmluZyxcbiAgICByZWNvcmRzOiBSZWNvcmQgfCBSZWNvcmRbXSxcbiAgICBleHRJZEZpZWxkOiBzdHJpbmcsXG4gICAgb3B0aW9uczogRG1sT3B0aW9ucyA9IHt9LFxuICApOiBQcm9taXNlPFNhdmVSZXN1bHQgfCBTYXZlUmVzdWx0W10+IHtcbiAgICBjb25zdCBpc0FycmF5ID0gQXJyYXkuaXNBcnJheShyZWNvcmRzKTtcbiAgICBjb25zdCBfcmVjb3JkcyA9IEFycmF5LmlzQXJyYXkocmVjb3JkcykgPyByZWNvcmRzIDogW3JlY29yZHNdO1xuICAgIGlmIChfcmVjb3Jkcy5sZW5ndGggPiB0aGlzLl9tYXhSZXF1ZXN0KSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoJ0V4Y2VlZGVkIG1heCBsaW1pdCBvZiBjb25jdXJyZW50IGNhbGwnKTtcbiAgICB9XG4gICAgY29uc3QgcmVzdWx0cyA9IGF3YWl0IFByb21pc2UuYWxsKFxuICAgICAgX3JlY29yZHMubWFwKChyZWNvcmQpID0+IHtcbiAgICAgICAgY29uc3QgeyBbZXh0SWRGaWVsZF06IGV4dElkLCB0eXBlOiBydHlwZSwgYXR0cmlidXRlcywgLi4ucmVjIH0gPSByZWNvcmQ7XG4gICAgICAgIGNvbnN0IHVybCA9IFt0aGlzLl9iYXNlVXJsKCksICdzb2JqZWN0cycsIHR5cGUsIGV4dElkRmllbGQsIGV4dElkXS5qb2luKFxuICAgICAgICAgICcvJyxcbiAgICAgICAgKTtcbiAgICAgICAgcmV0dXJuIHRoaXMucmVxdWVzdDxTYXZlUmVzdWx0PihcbiAgICAgICAgICB7XG4gICAgICAgICAgICBtZXRob2Q6ICdQQVRDSCcsXG4gICAgICAgICAgICB1cmwsXG4gICAgICAgICAgICBib2R5OiBKU09OLnN0cmluZ2lmeShyZWMpLFxuICAgICAgICAgICAgaGVhZGVyczoge1xuICAgICAgICAgICAgICAuLi4ob3B0aW9ucy5oZWFkZXJzIHx8IHt9KSxcbiAgICAgICAgICAgICAgJ2NvbnRlbnQtdHlwZSc6ICdhcHBsaWNhdGlvbi9qc29uJyxcbiAgICAgICAgICAgIH0sXG4gICAgICAgICAgfSxcbiAgICAgICAgICB7XG4gICAgICAgICAgICBub0NvbnRlbnRSZXNwb25zZTogeyBzdWNjZXNzOiB0cnVlLCBlcnJvcnM6IFtdIH0sXG4gICAgICAgICAgfSxcbiAgICAgICAgKS5jYXRjaCgoZXJyKSA9PiB7XG4gICAgICAgICAgLy8gQmUgYXdhcmUgdGhhdCBgYWxsT3JOb25lYCBvcHRpb24gaW4gdXBzZXJ0IG1ldGhvZFxuICAgICAgICAgIC8vIHdpbGwgbm90IHJldmVydCB0aGUgb3RoZXIgc3VjY2Vzc2Z1bCByZXF1ZXN0cy5cbiAgICAgICAgICAvLyBJdCBvbmx5IHJhaXNlcyBlcnJvciB3aGVuIG1ldCBhdCBsZWFzdCBvbmUgZmFpbGVkIHJlcXVlc3QuXG4gICAgICAgICAgaWYgKCFpc0FycmF5IHx8IG9wdGlvbnMuYWxsT3JOb25lIHx8ICFlcnIuZXJyb3JDb2RlKSB7XG4gICAgICAgICAgICB0aHJvdyBlcnI7XG4gICAgICAgICAgfVxuICAgICAgICAgIHJldHVybiB0b1NhdmVSZXN1bHQoZXJyKTtcbiAgICAgICAgfSk7XG4gICAgICB9KSxcbiAgICApO1xuICAgIHJldHVybiBpc0FycmF5ID8gcmVzdWx0cyA6IHJlc3VsdHNbMF07XG4gIH1cblxuICAvKipcbiAgICogRGVsZXRlIHJlY29yZHNcbiAgICovXG4gIGRlc3Ryb3k8TiBleHRlbmRzIFNPYmplY3ROYW1lczxTPj4oXG4gICAgdHlwZTogTixcbiAgICBpZHM6IHN0cmluZ1tdLFxuICAgIG9wdGlvbnM/OiBEbWxPcHRpb25zLFxuICApOiBQcm9taXNlPFNhdmVSZXN1bHRbXT47XG4gIGRlc3Ryb3k8TiBleHRlbmRzIFNPYmplY3ROYW1lczxTPj4oXG4gICAgdHlwZTogTixcbiAgICBpZDogc3RyaW5nLFxuICAgIG9wdGlvbnM/OiBEbWxPcHRpb25zLFxuICApOiBQcm9taXNlPFNhdmVSZXN1bHQ+O1xuICBkZXN0cm95PE4gZXh0ZW5kcyBTT2JqZWN0TmFtZXM8Uz4+KFxuICAgIHR5cGU6IE4sXG4gICAgaWRzOiBzdHJpbmcgfCBzdHJpbmdbXSxcbiAgICBvcHRpb25zPzogRG1sT3B0aW9ucyxcbiAgKTogUHJvbWlzZTxTYXZlUmVzdWx0IHwgU2F2ZVJlc3VsdFtdPjtcbiAgLyoqXG4gICAqIEBwYXJhbSB0eXBlXG4gICAqIEBwYXJhbSBpZHNcbiAgICogQHBhcmFtIG9wdGlvbnNcbiAgICovXG4gIGFzeW5jIGRlc3Ryb3koXG4gICAgdHlwZTogc3RyaW5nLFxuICAgIGlkczogc3RyaW5nIHwgc3RyaW5nW10sXG4gICAgb3B0aW9uczogRG1sT3B0aW9ucyA9IHt9LFxuICApOiBQcm9taXNlPFNhdmVSZXN1bHQgfCBTYXZlUmVzdWx0W10+IHtcbiAgICByZXR1cm4gQXJyYXkuaXNBcnJheShpZHMpXG4gICAgICA/IC8vIGNoZWNrIHRoZSB2ZXJzaW9uIHdoZXRoZXIgU09iamVjdCBjb2xsZWN0aW9uIEFQSSBpcyBzdXBwb3J0ZWQgKDQyLjApXG4gICAgICAgIHRoaXMuX2Vuc3VyZVZlcnNpb24oNDIpXG4gICAgICAgID8gdGhpcy5fZGVzdHJveU1hbnkodHlwZSwgaWRzLCBvcHRpb25zKVxuICAgICAgICA6IHRoaXMuX2Rlc3Ryb3lQYXJhbGxlbCh0eXBlLCBpZHMsIG9wdGlvbnMpXG4gICAgICA6IHRoaXMuX2Rlc3Ryb3lTaW5nbGUodHlwZSwgaWRzLCBvcHRpb25zKTtcbiAgfVxuXG4gIC8qKiBAcHJpdmF0ZSAqL1xuICBhc3luYyBfZGVzdHJveVNpbmdsZShcbiAgICB0eXBlOiBzdHJpbmcsXG4gICAgaWQ6IHN0cmluZyxcbiAgICBvcHRpb25zOiBEbWxPcHRpb25zLFxuICApOiBQcm9taXNlPFNhdmVSZXN1bHQ+IHtcbiAgICBjb25zdCB1cmwgPSBbdGhpcy5fYmFzZVVybCgpLCAnc29iamVjdHMnLCB0eXBlLCBpZF0uam9pbignLycpO1xuICAgIHJldHVybiB0aGlzLnJlcXVlc3QoXG4gICAgICB7XG4gICAgICAgIG1ldGhvZDogJ0RFTEVURScsXG4gICAgICAgIHVybCxcbiAgICAgICAgaGVhZGVyczogb3B0aW9ucy5oZWFkZXJzIHx8IHt9LFxuICAgICAgfSxcbiAgICAgIHtcbiAgICAgICAgbm9Db250ZW50UmVzcG9uc2U6IHsgaWQsIHN1Y2Nlc3M6IHRydWUsIGVycm9yczogW10gfSxcbiAgICAgIH0sXG4gICAgKTtcbiAgfVxuXG4gIC8qKiBAcHJpdmF0ZSAqL1xuICBhc3luYyBfZGVzdHJveVBhcmFsbGVsKHR5cGU6IHN0cmluZywgaWRzOiBzdHJpbmdbXSwgb3B0aW9uczogRG1sT3B0aW9ucykge1xuICAgIGlmIChpZHMubGVuZ3RoID4gdGhpcy5fbWF4UmVxdWVzdCkge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKCdFeGNlZWRlZCBtYXggbGltaXQgb2YgY29uY3VycmVudCBjYWxsJyk7XG4gICAgfVxuICAgIHJldHVybiBQcm9taXNlLmFsbChcbiAgICAgIGlkcy5tYXAoKGlkKSA9PlxuICAgICAgICB0aGlzLl9kZXN0cm95U2luZ2xlKHR5cGUsIGlkLCBvcHRpb25zKS5jYXRjaCgoZXJyKSA9PiB7XG4gICAgICAgICAgLy8gQmUgYXdhcmUgdGhhdCBgYWxsT3JOb25lYCBvcHRpb24gaW4gcGFyYWxsZWwgbW9kZVxuICAgICAgICAgIC8vIHdpbGwgbm90IHJldmVydCB0aGUgb3RoZXIgc3VjY2Vzc2Z1bCByZXF1ZXN0cy5cbiAgICAgICAgICAvLyBJdCBvbmx5IHJhaXNlcyBlcnJvciB3aGVuIG1ldCBhdCBsZWFzdCBvbmUgZmFpbGVkIHJlcXVlc3QuXG4gICAgICAgICAgaWYgKG9wdGlvbnMuYWxsT3JOb25lIHx8ICFlcnIuZXJyb3JDb2RlKSB7XG4gICAgICAgICAgICB0aHJvdyBlcnI7XG4gICAgICAgICAgfVxuICAgICAgICAgIHJldHVybiB0b1NhdmVSZXN1bHQoZXJyKTtcbiAgICAgICAgfSksXG4gICAgICApLFxuICAgICk7XG4gIH1cblxuICAvKiogQHByaXZhdGUgKi9cbiAgYXN5bmMgX2Rlc3Ryb3lNYW55KFxuICAgIHR5cGU6IHN0cmluZyxcbiAgICBpZHM6IHN0cmluZ1tdLFxuICAgIG9wdGlvbnM6IERtbE9wdGlvbnMsXG4gICk6IFByb21pc2U8U2F2ZVJlc3VsdFtdPiB7XG4gICAgaWYgKGlkcy5sZW5ndGggPT09IDApIHtcbiAgICAgIHJldHVybiBbXTtcbiAgICB9XG4gICAgaWYgKGlkcy5sZW5ndGggPiBNQVhfRE1MX0NPVU5UICYmIG9wdGlvbnMuYWxsb3dSZWN1cnNpdmUpIHtcbiAgICAgIHJldHVybiBbXG4gICAgICAgIC4uLihhd2FpdCB0aGlzLl9kZXN0cm95TWFueShcbiAgICAgICAgICB0eXBlLFxuICAgICAgICAgIGlkcy5zbGljZSgwLCBNQVhfRE1MX0NPVU5UKSxcbiAgICAgICAgICBvcHRpb25zLFxuICAgICAgICApKSxcbiAgICAgICAgLi4uKGF3YWl0IHRoaXMuX2Rlc3Ryb3lNYW55KHR5cGUsIGlkcy5zbGljZShNQVhfRE1MX0NPVU5UKSwgb3B0aW9ucykpLFxuICAgICAgXTtcbiAgICB9XG4gICAgbGV0IHVybCA9XG4gICAgICBbdGhpcy5fYmFzZVVybCgpLCAnY29tcG9zaXRlJywgJ3NvYmplY3RzP2lkcz0nXS5qb2luKCcvJykgKyBpZHMuam9pbignLCcpO1xuICAgIGlmIChvcHRpb25zLmFsbE9yTm9uZSkge1xuICAgICAgdXJsICs9ICcmYWxsT3JOb25lPXRydWUnO1xuICAgIH1cbiAgICByZXR1cm4gdGhpcy5yZXF1ZXN0KHtcbiAgICAgIG1ldGhvZDogJ0RFTEVURScsXG4gICAgICB1cmwsXG4gICAgICBoZWFkZXJzOiBvcHRpb25zLmhlYWRlcnMgfHwge30sXG4gICAgfSk7XG4gIH1cblxuICAvKipcbiAgICogU3lub255bSBvZiBDb25uZWN0aW9uI2Rlc3Ryb3koKVxuICAgKi9cbiAgZGVsZXRlID0gdGhpcy5kZXN0cm95O1xuXG4gIC8qKlxuICAgKiBTeW5vbnltIG9mIENvbm5lY3Rpb24jZGVzdHJveSgpXG4gICAqL1xuICBkZWwgPSB0aGlzLmRlc3Ryb3k7XG5cbiAgLyoqXG4gICAqIERlc2NyaWJlIFNPYmplY3QgbWV0YWRhdGFcbiAgICovXG4gIGFzeW5jIGRlc2NyaWJlKHR5cGU6IHN0cmluZyk6IFByb21pc2U8RGVzY3JpYmVTT2JqZWN0UmVzdWx0PiB7XG4gICAgY29uc3QgdXJsID0gW3RoaXMuX2Jhc2VVcmwoKSwgJ3NvYmplY3RzJywgdHlwZSwgJ2Rlc2NyaWJlJ10uam9pbignLycpO1xuICAgIGNvbnN0IGJvZHkgPSBhd2FpdCB0aGlzLnJlcXVlc3QodXJsKTtcbiAgICByZXR1cm4gYm9keSBhcyBEZXNjcmliZVNPYmplY3RSZXN1bHQ7XG4gIH1cblxuICAvKipcbiAgICogRGVzY3JpYmUgZ2xvYmFsIFNPYmplY3RzXG4gICAqL1xuICBhc3luYyBkZXNjcmliZUdsb2JhbCgpIHtcbiAgICBjb25zdCB1cmwgPSBgJHt0aGlzLl9iYXNlVXJsKCl9L3NvYmplY3RzYDtcbiAgICBjb25zdCBib2R5ID0gYXdhaXQgdGhpcy5yZXF1ZXN0KHVybCk7XG4gICAgcmV0dXJuIGJvZHkgYXMgRGVzY3JpYmVHbG9iYWxSZXN1bHQ7XG4gIH1cblxuICAvKipcbiAgICogR2V0IFNPYmplY3QgaW5zdGFuY2VcbiAgICovXG4gIHNvYmplY3Q8TiBleHRlbmRzIFNPYmplY3ROYW1lczxTPj4odHlwZTogc3RyaW5nfE4pOiBTT2JqZWN0PFMsIE4+O1xuICBzb2JqZWN0PE4gZXh0ZW5kcyBTT2JqZWN0TmFtZXM8Uz4+KHR5cGU6IE4gfCBzdHJpbmcpOiBTT2JqZWN0PFMsIE4+IHtcbiAgICBjb25zdCBzbyA9IHRoaXMuc29iamVjdHNbdHlwZSBhcyBOXSB8fCBuZXcgU09iamVjdCh0aGlzLCB0eXBlIGFzIE4pO1xuICAgIHRoaXMuc29iamVjdHNbdHlwZSBhcyBOXSA9IHNvO1xuICAgIHJldHVybiBzbztcbiAgfVxuXG4gIC8qKlxuICAgKiBHZXQgaWRlbnRpdHkgaW5mb3JtYXRpb24gb2YgY3VycmVudCB1c2VyXG4gICAqL1xuICBhc3luYyBpZGVudGl0eShvcHRpb25zOiB7IGhlYWRlcnM/OiB7IFtuYW1lOiBzdHJpbmddOiBzdHJpbmcgfSB9ID0ge30pIHtcbiAgICBsZXQgdXJsID0gdGhpcy51c2VySW5mbz8udXJsO1xuICAgIGlmICghdXJsKSB7XG4gICAgICBjb25zdCByZXMgPSBhd2FpdCB0aGlzLnJlcXVlc3Q8eyBpZGVudGl0eTogc3RyaW5nIH0+KHtcbiAgICAgICAgbWV0aG9kOiAnR0VUJyxcbiAgICAgICAgdXJsOiB0aGlzLl9iYXNlVXJsKCksXG4gICAgICAgIGhlYWRlcnM6IG9wdGlvbnMuaGVhZGVycyxcbiAgICAgIH0pO1xuICAgICAgdXJsID0gcmVzLmlkZW50aXR5O1xuICAgIH1cbiAgICB1cmwgKz0gJz9mb3JtYXQ9anNvbic7XG4gICAgaWYgKHRoaXMuYWNjZXNzVG9rZW4pIHtcbiAgICAgIHVybCArPSBgJm9hdXRoX3Rva2VuPSR7ZW5jb2RlVVJJQ29tcG9uZW50KHRoaXMuYWNjZXNzVG9rZW4pfWA7XG4gICAgfVxuICAgIGNvbnN0IHJlcyA9IGF3YWl0IHRoaXMucmVxdWVzdDxJZGVudGl0eUluZm8+KHsgbWV0aG9kOiAnR0VUJywgdXJsIH0pO1xuICAgIHRoaXMudXNlckluZm8gPSB7XG4gICAgICBpZDogcmVzLnVzZXJfaWQsXG4gICAgICBvcmdhbml6YXRpb25JZDogcmVzLm9yZ2FuaXphdGlvbl9pZCxcbiAgICAgIHVybDogcmVzLmlkLFxuICAgIH07XG4gICAgcmV0dXJuIHJlcztcbiAgfVxuXG4gIC8qKlxuICAgKiBMaXN0IHJlY2VudGx5IHZpZXdlZCByZWNvcmRzXG4gICAqL1xuICBhc3luYyByZWNlbnQodHlwZT86IHN0cmluZyB8IG51bWJlciwgbGltaXQ/OiBudW1iZXIpIHtcbiAgICAvKiBlc2xpbnQtZGlzYWJsZSBuby1wYXJhbS1yZWFzc2lnbiAqL1xuICAgIGlmICh0eXBlb2YgdHlwZSA9PT0gJ251bWJlcicpIHtcbiAgICAgIGxpbWl0ID0gdHlwZTtcbiAgICAgIHR5cGUgPSB1bmRlZmluZWQ7XG4gICAgfVxuICAgIGxldCB1cmw7XG4gICAgaWYgKHR5cGUpIHtcbiAgICAgIHVybCA9IFt0aGlzLl9iYXNlVXJsKCksICdzb2JqZWN0cycsIHR5cGVdLmpvaW4oJy8nKTtcbiAgICAgIGNvbnN0IHsgcmVjZW50SXRlbXMgfSA9IGF3YWl0IHRoaXMucmVxdWVzdDx7IHJlY2VudEl0ZW1zOiBSZWNvcmRbXSB9PihcbiAgICAgICAgdXJsLFxuICAgICAgKTtcbiAgICAgIHJldHVybiBsaW1pdCA/IHJlY2VudEl0ZW1zLnNsaWNlKDAsIGxpbWl0KSA6IHJlY2VudEl0ZW1zO1xuICAgIH1cbiAgICB1cmwgPSBgJHt0aGlzLl9iYXNlVXJsKCl9L3JlY2VudGA7XG4gICAgaWYgKGxpbWl0KSB7XG4gICAgICB1cmwgKz0gYD9saW1pdD0ke2xpbWl0fWA7XG4gICAgfVxuICAgIHJldHVybiB0aGlzLnJlcXVlc3Q8UmVjb3JkW10+KHVybCk7XG4gIH1cblxuICAvKipcbiAgICogUmV0cmlldmUgdXBkYXRlZCByZWNvcmRzXG4gICAqL1xuICBhc3luYyB1cGRhdGVkKFxuICAgIHR5cGU6IHN0cmluZyxcbiAgICBzdGFydDogc3RyaW5nIHwgRGF0ZSxcbiAgICBlbmQ6IHN0cmluZyB8IERhdGUsXG4gICk6IFByb21pc2U8VXBkYXRlZFJlc3VsdD4ge1xuICAgIC8qIGVzbGludC1kaXNhYmxlIG5vLXBhcmFtLXJlYXNzaWduICovXG4gICAgbGV0IHVybCA9IFt0aGlzLl9iYXNlVXJsKCksICdzb2JqZWN0cycsIHR5cGUsICd1cGRhdGVkJ10uam9pbignLycpO1xuICAgIGlmICh0eXBlb2Ygc3RhcnQgPT09ICdzdHJpbmcnKSB7XG4gICAgICBzdGFydCA9IG5ldyBEYXRlKHN0YXJ0KTtcbiAgICB9XG4gICAgc3RhcnQgPSBmb3JtYXREYXRlKHN0YXJ0KTtcbiAgICB1cmwgKz0gYD9zdGFydD0ke2VuY29kZVVSSUNvbXBvbmVudChzdGFydCl9YDtcbiAgICBpZiAodHlwZW9mIGVuZCA9PT0gJ3N0cmluZycpIHtcbiAgICAgIGVuZCA9IG5ldyBEYXRlKGVuZCk7XG4gICAgfVxuICAgIGVuZCA9IGZvcm1hdERhdGUoZW5kKTtcbiAgICB1cmwgKz0gYCZlbmQ9JHtlbmNvZGVVUklDb21wb25lbnQoZW5kKX1gO1xuICAgIGNvbnN0IGJvZHkgPSBhd2FpdCB0aGlzLnJlcXVlc3QodXJsKTtcbiAgICByZXR1cm4gYm9keSBhcyBVcGRhdGVkUmVzdWx0O1xuICB9XG5cbiAgLyoqXG4gICAqIFJldHJpZXZlIGRlbGV0ZWQgcmVjb3Jkc1xuICAgKi9cbiAgYXN5bmMgZGVsZXRlZChcbiAgICB0eXBlOiBzdHJpbmcsXG4gICAgc3RhcnQ6IHN0cmluZyB8IERhdGUsXG4gICAgZW5kOiBzdHJpbmcgfCBEYXRlLFxuICApOiBQcm9taXNlPERlbGV0ZWRSZXN1bHQ+IHtcbiAgICAvKiBlc2xpbnQtZGlzYWJsZSBuby1wYXJhbS1yZWFzc2lnbiAqL1xuICAgIGxldCB1cmwgPSBbdGhpcy5fYmFzZVVybCgpLCAnc29iamVjdHMnLCB0eXBlLCAnZGVsZXRlZCddLmpvaW4oJy8nKTtcbiAgICBpZiAodHlwZW9mIHN0YXJ0ID09PSAnc3RyaW5nJykge1xuICAgICAgc3RhcnQgPSBuZXcgRGF0ZShzdGFydCk7XG4gICAgfVxuICAgIHN0YXJ0ID0gZm9ybWF0RGF0ZShzdGFydCk7XG4gICAgdXJsICs9IGA/c3RhcnQ9JHtlbmNvZGVVUklDb21wb25lbnQoc3RhcnQpfWA7XG5cbiAgICBpZiAodHlwZW9mIGVuZCA9PT0gJ3N0cmluZycpIHtcbiAgICAgIGVuZCA9IG5ldyBEYXRlKGVuZCk7XG4gICAgfVxuICAgIGVuZCA9IGZvcm1hdERhdGUoZW5kKTtcbiAgICB1cmwgKz0gYCZlbmQ9JHtlbmNvZGVVUklDb21wb25lbnQoZW5kKX1gO1xuICAgIGNvbnN0IGJvZHkgPSBhd2FpdCB0aGlzLnJlcXVlc3QodXJsKTtcbiAgICByZXR1cm4gYm9keSBhcyBEZWxldGVkUmVzdWx0O1xuICB9XG5cbiAgLyoqXG4gICAqIFJldHVybnMgYSBsaXN0IG9mIGFsbCB0YWJzXG4gICAqL1xuICBhc3luYyB0YWJzKCk6IFByb21pc2U8RGVzY3JpYmVUYWJbXT4ge1xuICAgIGNvbnN0IHVybCA9IFt0aGlzLl9iYXNlVXJsKCksICd0YWJzJ10uam9pbignLycpO1xuICAgIGNvbnN0IGJvZHkgPSBhd2FpdCB0aGlzLnJlcXVlc3QodXJsKTtcbiAgICByZXR1cm4gYm9keSBhcyBEZXNjcmliZVRhYltdO1xuICB9XG5cbiAgLyoqXG4gICAqIFJldHVybnMgY3VycmVudCBzeXN0ZW0gbGltaXQgaW4gdGhlIG9yZ2FuaXphdGlvblxuICAgKi9cbiAgYXN5bmMgbGltaXRzKCk6IFByb21pc2U8T3JnYW5pemF0aW9uTGltaXRzSW5mbz4ge1xuICAgIGNvbnN0IHVybCA9IFt0aGlzLl9iYXNlVXJsKCksICdsaW1pdHMnXS5qb2luKCcvJyk7XG4gICAgY29uc3QgYm9keSA9IGF3YWl0IHRoaXMucmVxdWVzdCh1cmwpO1xuICAgIHJldHVybiBib2R5IGFzIE9yZ2FuaXphdGlvbkxpbWl0c0luZm87XG4gIH1cblxuICAvKipcbiAgICogUmV0dXJucyBhIHRoZW1lIGluZm9cbiAgICovXG4gIGFzeW5jIHRoZW1lKCk6IFByb21pc2U8RGVzY3JpYmVUaGVtZT4ge1xuICAgIGNvbnN0IHVybCA9IFt0aGlzLl9iYXNlVXJsKCksICd0aGVtZSddLmpvaW4oJy8nKTtcbiAgICBjb25zdCBib2R5ID0gYXdhaXQgdGhpcy5yZXF1ZXN0KHVybCk7XG4gICAgcmV0dXJuIGJvZHkgYXMgRGVzY3JpYmVUaGVtZTtcbiAgfVxuXG4gIC8qKlxuICAgKiBSZXR1cm5zIGFsbCByZWdpc3RlcmVkIGdsb2JhbCBxdWljayBhY3Rpb25zXG4gICAqL1xuICBhc3luYyBxdWlja0FjdGlvbnMoKTogUHJvbWlzZTxEZXNjcmliZVF1aWNrQWN0aW9uUmVzdWx0W10+IHtcbiAgICBjb25zdCBib2R5ID0gYXdhaXQgdGhpcy5yZXF1ZXN0KCcvcXVpY2tBY3Rpb25zJyk7XG4gICAgcmV0dXJuIGJvZHkgYXMgRGVzY3JpYmVRdWlja0FjdGlvblJlc3VsdFtdO1xuICB9XG5cbiAgLyoqXG4gICAqIEdldCByZWZlcmVuY2UgZm9yIHNwZWNpZmllZCBnbG9iYWwgcXVpY2sgYWN0aW9uXG4gICAqL1xuICBxdWlja0FjdGlvbihhY3Rpb25OYW1lOiBzdHJpbmcpOiBRdWlja0FjdGlvbjxTPiB7XG4gICAgcmV0dXJuIG5ldyBRdWlja0FjdGlvbih0aGlzLCBgL3F1aWNrQWN0aW9ucy8ke2FjdGlvbk5hbWV9YCk7XG4gIH1cblxuICAvKipcbiAgICogTW9kdWxlIHdoaWNoIG1hbmFnZXMgcHJvY2VzcyBydWxlcyBhbmQgYXBwcm92YWwgcHJvY2Vzc2VzXG4gICAqL1xuICBwcm9jZXNzID0gbmV3IFByb2Nlc3ModGhpcyk7XG5cbiAgcHJpdmF0ZSBpc0xpZ2h0bmluZ0luc3RhbmNlKCk6IGJvb2xlYW4ge1xuICAgIHJldHVybiAoXG4gICAgICB0aGlzLmluc3RhbmNlVXJsLmluY2x1ZGVzKCcubGlnaHRuaW5nLmZvcmNlLmNvbScpIHx8XG4gICAgICB0aGlzLmluc3RhbmNlVXJsLmluY2x1ZGVzKCcubGlnaHRuaW5nLmNybWZvcmNlLm1pbCcpIHx8XG4gICAgICB0aGlzLmluc3RhbmNlVXJsLmluY2x1ZGVzKCcubGlnaHRuaW5nLnNmY3JtYXBwcy5jbicpXG4gICAgKTtcbiAgfVxufVxuXG5leHBvcnQgZGVmYXVsdCBDb25uZWN0aW9uO1xuIl0sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7QUFBQTtBQUNBO0FBQ0E7QUFDQSxTQUFTQSxZQUFZLFFBQVEsUUFBUTtBQUNyQyxPQUFPQyxPQUFPLE1BQU0sV0FBVztBQWdDL0IsT0FBT0MsU0FBUyxJQUNkQyxlQUFlLEVBQ2ZDLGdCQUFnQixFQUNoQkMsa0JBQWtCLFFBQ2IsYUFBYTtBQUNwQixTQUFpQkMsU0FBUyxRQUFRLGVBQWU7QUFFakQsT0FBT0MsTUFBTSxNQUF5QixVQUFVO0FBRWhE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