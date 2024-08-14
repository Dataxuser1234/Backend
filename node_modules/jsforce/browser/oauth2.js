import _Object$keys from "@babel/runtime-corejs3/core-js-stable/object/keys";
import _Object$getOwnPropertySymbols from "@babel/runtime-corejs3/core-js-stable/object/get-own-property-symbols";
import _filterInstanceProperty from "@babel/runtime-corejs3/core-js-stable/instance/filter";
import _Object$getOwnPropertyDescriptor from "@babel/runtime-corejs3/core-js-stable/object/get-own-property-descriptor";
import _forEachInstanceProperty from "@babel/runtime-corejs3/core-js-stable/instance/for-each";
import _Object$getOwnPropertyDescriptors from "@babel/runtime-corejs3/core-js-stable/object/get-own-property-descriptors";
import _Object$defineProperties from "@babel/runtime-corejs3/core-js-stable/object/define-properties";
import _Object$defineProperty from "@babel/runtime-corejs3/core-js-stable/object/define-property";
import _Reflect$construct from "@babel/runtime-corejs3/core-js-stable/reflect/construct";
import _possibleConstructorReturn from "@babel/runtime-corejs3/helpers/possibleConstructorReturn";
import _getPrototypeOf from "@babel/runtime-corejs3/helpers/getPrototypeOf";
import _inherits from "@babel/runtime-corejs3/helpers/inherits";
import _wrapNativeSuper from "@babel/runtime-corejs3/helpers/wrapNativeSuper";
import _asyncToGenerator from "@babel/runtime-corejs3/helpers/asyncToGenerator";
import _defineProperty from "@babel/runtime-corejs3/helpers/defineProperty";
import _classCallCheck from "@babel/runtime-corejs3/helpers/classCallCheck";
import _createClass from "@babel/runtime-corejs3/helpers/createClass";
function _callSuper(t, o, e) { return o = _getPrototypeOf(o), _possibleConstructorReturn(t, _isNativeReflectConstruct() ? _Reflect$construct(o, e || [], _getPrototypeOf(t).constructor) : o.apply(t, e)); }
function _isNativeReflectConstruct() { try { var t = !Boolean.prototype.valueOf.call(_Reflect$construct(Boolean, [], function () {})); } catch (t) {} return (_isNativeReflectConstruct = function _isNativeReflectConstruct() { return !!t; })(); }
import _regeneratorRuntime from "@babel/runtime-corejs3/regenerator";
function ownKeys(e, r) { var t = _Object$keys(e); if (_Object$getOwnPropertySymbols) { var o = _Object$getOwnPropertySymbols(e); r && (o = _filterInstanceProperty(o).call(o, function (r) { return _Object$getOwnPropertyDescriptor(e, r).enumerable; })), t.push.apply(t, o); } return t; }
function _objectSpread(e) { for (var r = 1; r < arguments.length; r++) { var _context8, _context9; var t = null != arguments[r] ? arguments[r] : {}; r % 2 ? _forEachInstanceProperty(_context8 = ownKeys(Object(t), !0)).call(_context8, function (r) { _defineProperty(e, r, t[r]); }) : _Object$getOwnPropertyDescriptors ? _Object$defineProperties(e, _Object$getOwnPropertyDescriptors(t)) : _forEachInstanceProperty(_context9 = ownKeys(Object(t))).call(_context9, function (r) { _Object$defineProperty(e, r, _Object$getOwnPropertyDescriptor(t, r)); }); } return e; }
import _sliceInstanceProperty from "@babel/runtime-corejs3/core-js-stable/instance/slice";
import _URL from "@babel/runtime-corejs3/core-js-stable/url";
import _includesInstanceProperty from "@babel/runtime-corejs3/core-js-stable/instance/includes";
import "core-js/modules/es.error.cause.js";
import "core-js/modules/es.array.join.js";
import "core-js/modules/es.function.name.js";
import "core-js/modules/es.object.keys.js";
import "core-js/modules/es.array.push.js";
import "core-js/modules/es.object.to-string.js";
import "core-js/modules/es.regexp.exec.js";
import "core-js/modules/es.regexp.to-string.js";
import "core-js/modules/es.string.replace.js";
/**
 *
 */
import { createHash, randomBytes } from 'crypto';
import querystring from 'querystring';
import Transport, { HttpProxyTransport, XdProxyTransport } from './transport';
var defaultOAuth2Config = {
  loginUrl: 'https://login.salesforce.com'
};

// Makes a nodejs base64 encoded string compatible with rfc4648 alternative encoding for urls.
// @param base64Encoded a nodejs base64 encoded string
function base64UrlEscape(base64Encoded) {
  // builtin node js base 64 encoding is not 64 url compatible.
  // See https://toolsn.ietf.org/html/rfc4648#section-5
  return base64Encoded.replace(/\+/g, '-').replace(/\//g, '_').replace(/=/g, '');
}

/**
 * type defs
 */

/**
 * OAuth2 class
 */
export var OAuth2 = /*#__PURE__*/function () {
  /**
   *
   */
  function OAuth2(config) {
    _classCallCheck(this, OAuth2);
    var loginUrl = config.loginUrl,
      authzServiceUrl = config.authzServiceUrl,
      tokenServiceUrl = config.tokenServiceUrl,
      revokeServiceUrl = config.revokeServiceUrl,
      clientId = config.clientId,
      clientSecret = config.clientSecret,
      redirectUri = config.redirectUri,
      proxyUrl = config.proxyUrl,
      httpProxy = config.httpProxy,
      useVerifier = config.useVerifier;
    if (authzServiceUrl && tokenServiceUrl) {
      var _context;
      this.loginUrl = _sliceInstanceProperty(_context = authzServiceUrl.split('/')).call(_context, 0, 3).join('/');
      this.authzServiceUrl = authzServiceUrl;
      this.tokenServiceUrl = tokenServiceUrl;
      this.revokeServiceUrl = revokeServiceUrl || "".concat(this.loginUrl, "/services/oauth2/revoke");
    } else {
      var loginUrlObject = new _URL(loginUrl || defaultOAuth2Config.loginUrl);
      this.loginUrl = loginUrlObject.href;
      this.authzServiceUrl = "".concat(loginUrlObject.origin, "/services/oauth2/authorize");
      this.tokenServiceUrl = "".concat(loginUrlObject.origin, "/services/oauth2/token");
      this.revokeServiceUrl = "".concat(loginUrlObject.origin, "/services/oauth2/revoke");
    }
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.redirectUri = redirectUri;
    if (proxyUrl) {
      this._transport = new XdProxyTransport(proxyUrl);
    } else if (httpProxy) {
      this._transport = new HttpProxyTransport(httpProxy);
    } else {
      this._transport = new Transport();
    }
    if (useVerifier) {
      // Set a code verifier string for OAuth authorization
      this.codeVerifier = base64UrlEscape(randomBytes(Math.ceil(128)).toString('base64'));
    }
  }

  /**
   * Get Salesforce OAuth2 authorization page URL to redirect user agent.
   */
  return _createClass(OAuth2, [{
    key: "getAuthorizationUrl",
    value: function getAuthorizationUrl() {
      var _context2;
      var params = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : {};
      if (this.codeVerifier) {
        // code verifier must be a base 64 url encoded hash of 128 bytes of random data. Our random data is also
        // base 64 url encoded. See Connection.create();
        params.code_challenge = base64UrlEscape(createHash('sha256').update(this.codeVerifier).digest('base64'));
      }
      var _params = _objectSpread(_objectSpread({}, params), {}, {
        response_type: 'code',
        client_id: this.clientId,
        redirect_uri: this.redirectUri
      });
      return this.authzServiceUrl + (_includesInstanceProperty(_context2 = this.authzServiceUrl).call(_context2, '?') ? '&' : '?') + querystring.stringify(_params);
    }

    /**
     * OAuth2 Refresh Token Flow
     */
  }, {
    key: "refreshToken",
    value: (function () {
      var _refreshToken2 = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee(_refreshToken) {
        var params, ret;
        return _regeneratorRuntime.wrap(function _callee$(_context3) {
          while (1) switch (_context3.prev = _context3.next) {
            case 0:
              if (this.clientId) {
                _context3.next = 2;
                break;
              }
              throw new Error('No OAuth2 client id information is specified');
            case 2:
              params = {
                grant_type: 'refresh_token',
                refresh_token: _refreshToken,
                client_id: this.clientId
              };
              if (this.clientSecret) {
                params.client_secret = this.clientSecret;
              }
              _context3.next = 6;
              return this._postParams(params);
            case 6:
              ret = _context3.sent;
              return _context3.abrupt("return", ret);
            case 8:
            case "end":
              return _context3.stop();
          }
        }, _callee, this);
      }));
      function refreshToken(_x) {
        return _refreshToken2.apply(this, arguments);
      }
      return refreshToken;
    }()
    /**
     * Send access token request to the token endpoint.
     * When a code (string) is passed in first argument, it will use Web Server Authentication Flow (Authorization Code Grant).
     * Otherwise, it will use the specified `grant_type` and pass parameters to the endpoint.
     */
    )
  }, {
    key: "requestToken",
    value: (function () {
      var _requestToken = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee2(codeOrParams) {
        var params,
          _params,
          ret,
          _args2 = arguments;
        return _regeneratorRuntime.wrap(function _callee2$(_context4) {
          while (1) switch (_context4.prev = _context4.next) {
            case 0:
              params = _args2.length > 1 && _args2[1] !== undefined ? _args2[1] : {};
              if (!(typeof codeOrParams === 'string' && (!this.clientId || !this.redirectUri))) {
                _context4.next = 3;
                break;
              }
              throw new Error('No OAuth2 client id or redirect uri configuration is specified');
            case 3:
              _params = _objectSpread(_objectSpread({}, params), typeof codeOrParams === 'string' ? {
                grant_type: 'authorization_code',
                code: codeOrParams
              } : codeOrParams);
              if (this.clientId) {
                _params.client_id = this.clientId;
              }
              if (this.clientSecret) {
                _params.client_secret = this.clientSecret;
              }
              if (this.redirectUri) {
                _params.redirect_uri = this.redirectUri;
              }
              _context4.next = 9;
              return this._postParams(_params);
            case 9:
              ret = _context4.sent;
              return _context4.abrupt("return", ret);
            case 11:
            case "end":
              return _context4.stop();
          }
        }, _callee2, this);
      }));
      function requestToken(_x2) {
        return _requestToken.apply(this, arguments);
      }
      return requestToken;
    }()
    /**
     * OAuth2 Username-Password Flow (Resource Owner Password Credentials)
     */
    )
  }, {
    key: "authenticate",
    value: (function () {
      var _authenticate = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee3(username, password) {
        var ret;
        return _regeneratorRuntime.wrap(function _callee3$(_context5) {
          while (1) switch (_context5.prev = _context5.next) {
            case 0:
              if (!(!this.clientId || !this.clientSecret)) {
                _context5.next = 2;
                break;
              }
              throw new Error('No valid OAuth2 client configuration set');
            case 2:
              _context5.next = 4;
              return this._postParams({
                grant_type: 'password',
                username: username,
                password: password,
                client_id: this.clientId,
                client_secret: this.clientSecret
              });
            case 4:
              ret = _context5.sent;
              return _context5.abrupt("return", ret);
            case 6:
            case "end":
              return _context5.stop();
          }
        }, _callee3, this);
      }));
      function authenticate(_x3, _x4) {
        return _authenticate.apply(this, arguments);
      }
      return authenticate;
    }()
    /**
     * OAuth2 Revoke Session Token
     */
    )
  }, {
    key: "revokeToken",
    value: (function () {
      var _revokeToken = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee4(token) {
        var response, res;
        return _regeneratorRuntime.wrap(function _callee4$(_context6) {
          while (1) switch (_context6.prev = _context6.next) {
            case 0:
              _context6.next = 2;
              return this._transport.httpRequest({
                method: 'POST',
                url: this.revokeServiceUrl,
                body: querystring.stringify({
                  token: token
                }),
                headers: {
                  'content-type': 'application/x-www-form-urlencoded'
                }
              });
            case 2:
              response = _context6.sent;
              if (!(response.statusCode >= 400)) {
                _context6.next = 7;
                break;
              }
              res = querystring.parse(response.body);
              if (!res || !res.error) {
                res = {
                  error: "ERROR_HTTP_".concat(response.statusCode),
                  error_description: response.body
                };
              }
              throw new ( /*#__PURE__*/function (_Error) {
                function _class(_ref) {
                  var _this;
                  var error = _ref.error,
                    error_description = _ref.error_description;
                  _classCallCheck(this, _class);
                  _this = _callSuper(this, _class, [error_description]);
                  _this.name = error;
                  return _this;
                }
                _inherits(_class, _Error);
                return _createClass(_class);
              }( /*#__PURE__*/_wrapNativeSuper(Error)))(res);
            case 7:
            case "end":
              return _context6.stop();
          }
        }, _callee4, this);
      }));
      function revokeToken(_x5) {
        return _revokeToken.apply(this, arguments);
      }
      return revokeToken;
    }()
    /**
     * @private
     */
    )
  }, {
    key: "_postParams",
    value: (function () {
      var _postParams2 = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee5(params) {
        var response, res;
        return _regeneratorRuntime.wrap(function _callee5$(_context7) {
          while (1) switch (_context7.prev = _context7.next) {
            case 0:
              if (this.codeVerifier) params.code_verifier = this.codeVerifier;
              _context7.next = 3;
              return this._transport.httpRequest({
                method: 'POST',
                url: this.tokenServiceUrl,
                body: querystring.stringify(params),
                headers: {
                  'content-type': 'application/x-www-form-urlencoded'
                }
              });
            case 3:
              response = _context7.sent;
              try {
                res = JSON.parse(response.body);
              } catch (e) {
                /* eslint-disable no-empty */
              }
              if (!(response.statusCode >= 400)) {
                _context7.next = 8;
                break;
              }
              res = res || {
                error: "ERROR_HTTP_".concat(response.statusCode),
                error_description: response.body
              };
              throw new ( /*#__PURE__*/function (_Error2) {
                function _class2(_ref2) {
                  var _this2;
                  var error = _ref2.error,
                    error_description = _ref2.error_description;
                  _classCallCheck(this, _class2);
                  _this2 = _callSuper(this, _class2, [error_description]);
                  _this2.name = error;
                  return _this2;
                }
                _inherits(_class2, _Error2);
                return _createClass(_class2);
              }( /*#__PURE__*/_wrapNativeSuper(Error)))(res);
            case 8:
              return _context7.abrupt("return", res);
            case 9:
            case "end":
              return _context7.stop();
          }
        }, _callee5, this);
      }));
      function _postParams(_x6) {
        return _postParams2.apply(this, arguments);
      }
      return _postParams;
    }())
  }]);
}();
export default OAuth2;
//# sourceMappingURL=data:application/json;charset=utf-8;base64,eyJ2ZXJzaW9uIjozLCJuYW1lcyI6WyJjcmVhdGVIYXNoIiwicmFuZG9tQnl0ZXMiLCJxdWVyeXN0cmluZyIsIlRyYW5zcG9ydCIsIkh0dHBQcm94eVRyYW5zcG9ydCIsIlhkUHJveHlUcmFuc3BvcnQiLCJkZWZhdWx0T0F1dGgyQ29uZmlnIiwibG9naW5VcmwiLCJiYXNlNjRVcmxFc2NhcGUiLCJiYXNlNjRFbmNvZGVkIiwicmVwbGFjZSIsIk9BdXRoMiIsImNvbmZpZyIsIl9jbGFzc0NhbGxDaGVjayIsImF1dGh6U2VydmljZVVybCIsInRva2VuU2VydmljZVVybCIsInJldm9rZVNlcnZpY2VVcmwiLCJjbGllbnRJZCIsImNsaWVudFNlY3JldCIsInJlZGlyZWN0VXJpIiwicHJveHlVcmwiLCJodHRwUHJveHkiLCJ1c2VWZXJpZmllciIsIl9jb250ZXh0IiwiX3NsaWNlSW5zdGFuY2VQcm9wZXJ0eSIsInNwbGl0IiwiY2FsbCIsImpvaW4iLCJjb25jYXQiLCJsb2dpblVybE9iamVjdCIsIl9VUkwiLCJocmVmIiwib3JpZ2luIiwiX3RyYW5zcG9ydCIsImNvZGVWZXJpZmllciIsIk1hdGgiLCJjZWlsIiwidG9TdHJpbmciLCJfY3JlYXRlQ2xhc3MiLCJrZXkiLCJ2YWx1ZSIsImdldEF1dGhvcml6YXRpb25VcmwiLCJfY29udGV4dDIiLCJwYXJhbXMiLCJhcmd1bWVudHMiLCJsZW5ndGgiLCJ1bmRlZmluZWQiLCJjb2RlX2NoYWxsZW5nZSIsInVwZGF0ZSIsImRpZ2VzdCIsIl9wYXJhbXMiLCJfb2JqZWN0U3ByZWFkIiwicmVzcG9uc2VfdHlwZSIsImNsaWVudF9pZCIsInJlZGlyZWN0X3VyaSIsIl9pbmNsdWRlc0luc3RhbmNlUHJvcGVydHkiLCJzdHJpbmdpZnkiLCJfcmVmcmVzaFRva2VuMiIsIl9hc3luY1RvR2VuZXJhdG9yIiwiX3JlZ2VuZXJhdG9yUnVudGltZSIsIm1hcmsiLCJfY2FsbGVlIiwicmVmcmVzaFRva2VuIiwicmV0Iiwid3JhcCIsIl9jYWxsZWUkIiwiX2NvbnRleHQzIiwicHJldiIsIm5leHQiLCJFcnJvciIsImdyYW50X3R5cGUiLCJyZWZyZXNoX3Rva2VuIiwiY2xpZW50X3NlY3JldCIsIl9wb3N0UGFyYW1zIiwic2VudCIsImFicnVwdCIsInN0b3AiLCJfeCIsImFwcGx5IiwiX3JlcXVlc3RUb2tlbiIsIl9jYWxsZWUyIiwiY29kZU9yUGFyYW1zIiwiX2FyZ3MyIiwiX2NhbGxlZTIkIiwiX2NvbnRleHQ0IiwiY29kZSIsInJlcXVlc3RUb2tlbiIsIl94MiIsIl9hdXRoZW50aWNhdGUiLCJfY2FsbGVlMyIsInVzZXJuYW1lIiwicGFzc3dvcmQiLCJfY2FsbGVlMyQiLCJfY29udGV4dDUiLCJhdXRoZW50aWNhdGUiLCJfeDMiLCJfeDQiLCJfcmV2b2tlVG9rZW4iLCJfY2FsbGVlNCIsInRva2VuIiwicmVzcG9uc2UiLCJyZXMiLCJfY2FsbGVlNCQiLCJfY29udGV4dDYiLCJodHRwUmVxdWVzdCIsIm1ldGhvZCIsInVybCIsImJvZHkiLCJoZWFkZXJzIiwic3RhdHVzQ29kZSIsInBhcnNlIiwiZXJyb3IiLCJlcnJvcl9kZXNjcmlwdGlvbiIsIl9FcnJvciIsIl9jbGFzcyIsIl9yZWYiLCJfdGhpcyIsIl9jYWxsU3VwZXIiLCJuYW1lIiwiX2luaGVyaXRzIiwiX3dyYXBOYXRpdmVTdXBlciIsInJldm9rZVRva2VuIiwiX3g1IiwiX3Bvc3RQYXJhbXMyIiwiX2NhbGxlZTUiLCJfY2FsbGVlNSQiLCJfY29udGV4dDciLCJjb2RlX3ZlcmlmaWVyIiwiSlNPTiIsImUiLCJfRXJyb3IyIiwiX2NsYXNzMiIsIl9yZWYyIiwiX3RoaXMyIiwiX3g2Il0sInNvdXJjZXMiOlsiLi4vc3JjL29hdXRoMi50cyJdLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqXG4gKi9cbmltcG9ydCB7Y3JlYXRlSGFzaCwgcmFuZG9tQnl0ZXN9IGZyb20gJ2NyeXB0byc7XG5pbXBvcnQgcXVlcnlzdHJpbmcgZnJvbSAncXVlcnlzdHJpbmcnO1xuaW1wb3J0IFRyYW5zcG9ydCwge0h0dHBQcm94eVRyYW5zcG9ydCwgWGRQcm94eVRyYW5zcG9ydH0gZnJvbSAnLi90cmFuc3BvcnQnO1xuaW1wb3J0IHtPcHRpb25hbH0gZnJvbSAnLi90eXBlcyc7XG5cbmNvbnN0IGRlZmF1bHRPQXV0aDJDb25maWcgPSB7XG4gIGxvZ2luVXJsOiAnaHR0cHM6Ly9sb2dpbi5zYWxlc2ZvcmNlLmNvbScsXG59O1xuXG4vLyBNYWtlcyBhIG5vZGVqcyBiYXNlNjQgZW5jb2RlZCBzdHJpbmcgY29tcGF0aWJsZSB3aXRoIHJmYzQ2NDggYWx0ZXJuYXRpdmUgZW5jb2RpbmcgZm9yIHVybHMuXG4vLyBAcGFyYW0gYmFzZTY0RW5jb2RlZCBhIG5vZGVqcyBiYXNlNjQgZW5jb2RlZCBzdHJpbmdcbmZ1bmN0aW9uIGJhc2U2NFVybEVzY2FwZShiYXNlNjRFbmNvZGVkOiBzdHJpbmcpOiBzdHJpbmcge1xuICAvLyBidWlsdGluIG5vZGUganMgYmFzZSA2NCBlbmNvZGluZyBpcyBub3QgNjQgdXJsIGNvbXBhdGlibGUuXG4gIC8vIFNlZSBodHRwczovL3Rvb2xzbi5pZXRmLm9yZy9odG1sL3JmYzQ2NDgjc2VjdGlvbi01XG4gIHJldHVybiBiYXNlNjRFbmNvZGVkXG4gICAgLnJlcGxhY2UoL1xcKy9nLCAnLScpXG4gICAgLnJlcGxhY2UoL1xcLy9nLCAnXycpXG4gICAgLnJlcGxhY2UoLz0vZywgJycpO1xufVxuXG4vKipcbiAqIHR5cGUgZGVmc1xuICovXG5leHBvcnQgdHlwZSBPQXV0aDJDb25maWcgPSB7XG4gIGNsaWVudElkPzogc3RyaW5nO1xuICBjbGllbnRTZWNyZXQ/OiBzdHJpbmc7XG4gIHJlZGlyZWN0VXJpPzogc3RyaW5nO1xuICBsb2dpblVybD86IHN0cmluZztcbiAgYXV0aHpTZXJ2aWNlVXJsPzogc3RyaW5nO1xuICB0b2tlblNlcnZpY2VVcmw/OiBzdHJpbmc7XG4gIHJldm9rZVNlcnZpY2VVcmw/OiBzdHJpbmc7XG4gIHByb3h5VXJsPzogc3RyaW5nO1xuICBodHRwUHJveHk/OiBzdHJpbmc7XG4gIHVzZVZlcmlmaWVyPzogYm9vbGVhbjtcbn07XG5cbmV4cG9ydCB0eXBlIEF1dGh6UmVxdWVzdFBhcmFtcyA9IHtcbiAgc2NvcGU/OiBzdHJpbmc7XG4gIHN0YXRlPzogc3RyaW5nO1xuICBjb2RlX2NoYWxsZW5nZT86IHN0cmluZztcbn0gJiB7XG4gIFthdHRyOiBzdHJpbmddOiBzdHJpbmc7XG59O1xuXG5leHBvcnQgdHlwZSBUb2tlblJlc3BvbnNlID0ge1xuICB0b2tlbl90eXBlOiAnQmVhcmVyJztcbiAgLyoqXG4gICAqIElkZW50aXR5IFVSTFxuICAgKlxuICAgKiBUaGUgZm9ybWF0IG9mIHRoZSBVUkwgaXMgaHR0cHM6Ly9sb2dpbi5zYWxlc2ZvcmNlLmNvbS9pZC9vcmdJRC91c2VySUQuXG4gICAqL1xuICBpZDogc3RyaW5nO1xuICBhY2Nlc3NfdG9rZW46IHN0cmluZztcbiAgcmVmcmVzaF90b2tlbj86IHN0cmluZztcbiAgc2lnbmF0dXJlOiBzdHJpbmc7XG4gIGlzc3VlZF9hdDogc3RyaW5nO1xuICBpbnN0YW5jZV91cmw6IHN0cmluZztcbiAgc2ZkY19jb21tdW5pdHlfdXJsPzogc3RyaW5nO1xuICBzZmRjX2NvbW11bml0eV9pZD86IHN0cmluZztcbn07XG5cbi8qKlxuICogT0F1dGgyIGNsYXNzXG4gKi9cbmV4cG9ydCBjbGFzcyBPQXV0aDIge1xuICBsb2dpblVybDogc3RyaW5nO1xuICBhdXRoelNlcnZpY2VVcmw6IHN0cmluZztcbiAgdG9rZW5TZXJ2aWNlVXJsOiBzdHJpbmc7XG4gIHJldm9rZVNlcnZpY2VVcmw6IHN0cmluZztcbiAgY2xpZW50SWQ6IE9wdGlvbmFsPHN0cmluZz47XG4gIGNsaWVudFNlY3JldDogT3B0aW9uYWw8c3RyaW5nPjtcbiAgcmVkaXJlY3RVcmk6IE9wdGlvbmFsPHN0cmluZz47XG4gIGNvZGVWZXJpZmllcjogT3B0aW9uYWw8c3RyaW5nPjtcblxuICBfdHJhbnNwb3J0OiBUcmFuc3BvcnQ7XG5cbiAgLyoqXG4gICAqXG4gICAqL1xuICBjb25zdHJ1Y3Rvcihjb25maWc6IE9BdXRoMkNvbmZpZykge1xuICAgIGNvbnN0IHtcbiAgICAgIGxvZ2luVXJsLFxuICAgICAgYXV0aHpTZXJ2aWNlVXJsLFxuICAgICAgdG9rZW5TZXJ2aWNlVXJsLFxuICAgICAgcmV2b2tlU2VydmljZVVybCxcbiAgICAgIGNsaWVudElkLFxuICAgICAgY2xpZW50U2VjcmV0LFxuICAgICAgcmVkaXJlY3RVcmksXG4gICAgICBwcm94eVVybCxcbiAgICAgIGh0dHBQcm94eSxcbiAgICAgIHVzZVZlcmlmaWVyLFxuICAgIH0gPSBjb25maWc7XG4gICAgaWYgKGF1dGh6U2VydmljZVVybCAmJiB0b2tlblNlcnZpY2VVcmwpIHtcbiAgICAgIHRoaXMubG9naW5VcmwgPSBhdXRoelNlcnZpY2VVcmwuc3BsaXQoJy8nKS5zbGljZSgwLCAzKS5qb2luKCcvJyk7XG4gICAgICB0aGlzLmF1dGh6U2VydmljZVVybCA9IGF1dGh6U2VydmljZVVybDtcbiAgICAgIHRoaXMudG9rZW5TZXJ2aWNlVXJsID0gdG9rZW5TZXJ2aWNlVXJsO1xuICAgICAgdGhpcy5yZXZva2VTZXJ2aWNlVXJsID1cbiAgICAgICAgcmV2b2tlU2VydmljZVVybCB8fCBgJHt0aGlzLmxvZ2luVXJsfS9zZXJ2aWNlcy9vYXV0aDIvcmV2b2tlYDtcbiAgICB9IGVsc2Uge1xuICAgICAgY29uc3QgbG9naW5VcmxPYmplY3QgPSBuZXcgVVJMKGxvZ2luVXJsIHx8IGRlZmF1bHRPQXV0aDJDb25maWcubG9naW5VcmwpO1xuICAgICAgdGhpcy5sb2dpblVybCA9IGxvZ2luVXJsT2JqZWN0LmhyZWZcbiAgICAgIHRoaXMuYXV0aHpTZXJ2aWNlVXJsID0gYCR7bG9naW5VcmxPYmplY3Qub3JpZ2lufS9zZXJ2aWNlcy9vYXV0aDIvYXV0aG9yaXplYDtcbiAgICAgIHRoaXMudG9rZW5TZXJ2aWNlVXJsID0gYCR7bG9naW5VcmxPYmplY3Qub3JpZ2lufS9zZXJ2aWNlcy9vYXV0aDIvdG9rZW5gO1xuICAgICAgdGhpcy5yZXZva2VTZXJ2aWNlVXJsID0gYCR7bG9naW5VcmxPYmplY3Qub3JpZ2lufS9zZXJ2aWNlcy9vYXV0aDIvcmV2b2tlYDtcbiAgICB9XG4gICAgdGhpcy5jbGllbnRJZCA9IGNsaWVudElkO1xuICAgIHRoaXMuY2xpZW50U2VjcmV0ID0gY2xpZW50U2VjcmV0O1xuICAgIHRoaXMucmVkaXJlY3RVcmkgPSByZWRpcmVjdFVyaTtcbiAgICBpZiAocHJveHlVcmwpIHtcbiAgICAgIHRoaXMuX3RyYW5zcG9ydCA9IG5ldyBYZFByb3h5VHJhbnNwb3J0KHByb3h5VXJsKTtcbiAgICB9IGVsc2UgaWYgKGh0dHBQcm94eSkge1xuICAgICAgdGhpcy5fdHJhbnNwb3J0ID0gbmV3IEh0dHBQcm94eVRyYW5zcG9ydChodHRwUHJveHkpO1xuICAgIH0gZWxzZSB7XG4gICAgICB0aGlzLl90cmFuc3BvcnQgPSBuZXcgVHJhbnNwb3J0KCk7XG4gICAgfVxuICAgIGlmICh1c2VWZXJpZmllcikge1xuICAgICAgLy8gU2V0IGEgY29kZSB2ZXJpZmllciBzdHJpbmcgZm9yIE9BdXRoIGF1dGhvcml6YXRpb25cbiAgICAgIHRoaXMuY29kZVZlcmlmaWVyID0gYmFzZTY0VXJsRXNjYXBlKFxuICAgICAgICByYW5kb21CeXRlcyhNYXRoLmNlaWwoMTI4KSkudG9TdHJpbmcoJ2Jhc2U2NCcpLFxuICAgICAgKTtcbiAgICB9XG4gIH1cblxuICAvKipcbiAgICogR2V0IFNhbGVzZm9yY2UgT0F1dGgyIGF1dGhvcml6YXRpb24gcGFnZSBVUkwgdG8gcmVkaXJlY3QgdXNlciBhZ2VudC5cbiAgICovXG4gIGdldEF1dGhvcml6YXRpb25VcmwocGFyYW1zOiBBdXRoelJlcXVlc3RQYXJhbXMgPSB7fSkge1xuICAgIGlmICh0aGlzLmNvZGVWZXJpZmllcikge1xuICAgICAgLy8gY29kZSB2ZXJpZmllciBtdXN0IGJlIGEgYmFzZSA2NCB1cmwgZW5jb2RlZCBoYXNoIG9mIDEyOCBieXRlcyBvZiByYW5kb20gZGF0YS4gT3VyIHJhbmRvbSBkYXRhIGlzIGFsc29cbiAgICAgIC8vIGJhc2UgNjQgdXJsIGVuY29kZWQuIFNlZSBDb25uZWN0aW9uLmNyZWF0ZSgpO1xuICAgICAgcGFyYW1zLmNvZGVfY2hhbGxlbmdlID0gYmFzZTY0VXJsRXNjYXBlKFxuICAgICAgICBjcmVhdGVIYXNoKCdzaGEyNTYnKS51cGRhdGUodGhpcy5jb2RlVmVyaWZpZXIpLmRpZ2VzdCgnYmFzZTY0JyksXG4gICAgICApO1xuICAgIH1cblxuICAgIGNvbnN0IF9wYXJhbXMgPSB7XG4gICAgICAuLi5wYXJhbXMsXG4gICAgICByZXNwb25zZV90eXBlOiAnY29kZScsXG4gICAgICBjbGllbnRfaWQ6IHRoaXMuY2xpZW50SWQsXG4gICAgICByZWRpcmVjdF91cmk6IHRoaXMucmVkaXJlY3RVcmksXG4gICAgfTtcbiAgICByZXR1cm4gKFxuICAgICAgdGhpcy5hdXRoelNlcnZpY2VVcmwgK1xuICAgICAgKHRoaXMuYXV0aHpTZXJ2aWNlVXJsLmluY2x1ZGVzKCc/JykgPyAnJicgOiAnPycpICtcbiAgICAgIHF1ZXJ5c3RyaW5nLnN0cmluZ2lmeShfcGFyYW1zIGFzIHsgW25hbWU6IHN0cmluZ106IGFueSB9KVxuICAgICk7XG4gIH1cblxuICAvKipcbiAgICogT0F1dGgyIFJlZnJlc2ggVG9rZW4gRmxvd1xuICAgKi9cbiAgYXN5bmMgcmVmcmVzaFRva2VuKHJlZnJlc2hUb2tlbjogc3RyaW5nKTogUHJvbWlzZTxUb2tlblJlc3BvbnNlPiB7XG4gICAgaWYgKCF0aGlzLmNsaWVudElkKSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoJ05vIE9BdXRoMiBjbGllbnQgaWQgaW5mb3JtYXRpb24gaXMgc3BlY2lmaWVkJyk7XG4gICAgfVxuICAgIGNvbnN0IHBhcmFtczogeyBbcHJvcDogc3RyaW5nXTogc3RyaW5nIH0gPSB7XG4gICAgICBncmFudF90eXBlOiAncmVmcmVzaF90b2tlbicsXG4gICAgICByZWZyZXNoX3Rva2VuOiByZWZyZXNoVG9rZW4sXG4gICAgICBjbGllbnRfaWQ6IHRoaXMuY2xpZW50SWQsXG4gICAgfTtcbiAgICBpZiAodGhpcy5jbGllbnRTZWNyZXQpIHtcbiAgICAgIHBhcmFtcy5jbGllbnRfc2VjcmV0ID0gdGhpcy5jbGllbnRTZWNyZXQ7XG4gICAgfVxuICAgIGNvbnN0IHJldCA9IGF3YWl0IHRoaXMuX3Bvc3RQYXJhbXMocGFyYW1zKTtcbiAgICByZXR1cm4gcmV0IGFzIFRva2VuUmVzcG9uc2U7XG4gIH1cblxuICAvKipcbiAgICogU2VuZCBhY2Nlc3MgdG9rZW4gcmVxdWVzdCB0byB0aGUgdG9rZW4gZW5kcG9pbnQuXG4gICAqIFdoZW4gYSBjb2RlIChzdHJpbmcpIGlzIHBhc3NlZCBpbiBmaXJzdCBhcmd1bWVudCwgaXQgd2lsbCB1c2UgV2ViIFNlcnZlciBBdXRoZW50aWNhdGlvbiBGbG93IChBdXRob3JpemF0aW9uIENvZGUgR3JhbnQpLlxuICAgKiBPdGhlcndpc2UsIGl0IHdpbGwgdXNlIHRoZSBzcGVjaWZpZWQgYGdyYW50X3R5cGVgIGFuZCBwYXNzIHBhcmFtZXRlcnMgdG8gdGhlIGVuZHBvaW50LlxuICAgKi9cbiAgYXN5bmMgcmVxdWVzdFRva2VuKFxuICAgIGNvZGVPclBhcmFtczogc3RyaW5nIHwgeyBncmFudF90eXBlOiBzdHJpbmc7IFtuYW1lOiBzdHJpbmddOiBzdHJpbmcgfSxcbiAgICBwYXJhbXM6IHsgW3Byb3A6IHN0cmluZ106IHN0cmluZyB9ID0ge30sXG4gICk6IFByb21pc2U8VG9rZW5SZXNwb25zZT4ge1xuICAgIGlmIChcbiAgICAgIHR5cGVvZiBjb2RlT3JQYXJhbXMgPT09ICdzdHJpbmcnICYmXG4gICAgICAoIXRoaXMuY2xpZW50SWQgfHwgIXRoaXMucmVkaXJlY3RVcmkpXG4gICAgKSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoXG4gICAgICAgICdObyBPQXV0aDIgY2xpZW50IGlkIG9yIHJlZGlyZWN0IHVyaSBjb25maWd1cmF0aW9uIGlzIHNwZWNpZmllZCcsXG4gICAgICApO1xuICAgIH1cbiAgICBjb25zdCBfcGFyYW1zOiB7IFtwcm9wOiBzdHJpbmddOiBzdHJpbmcgfSA9IHtcbiAgICAgIC4uLnBhcmFtcyxcbiAgICAgIC4uLih0eXBlb2YgY29kZU9yUGFyYW1zID09PSAnc3RyaW5nJ1xuICAgICAgICA/IHsgZ3JhbnRfdHlwZTogJ2F1dGhvcml6YXRpb25fY29kZScsIGNvZGU6IGNvZGVPclBhcmFtcyB9XG4gICAgICAgIDogY29kZU9yUGFyYW1zKSxcbiAgICB9O1xuICAgIGlmICh0aGlzLmNsaWVudElkKSB7XG4gICAgICBfcGFyYW1zLmNsaWVudF9pZCA9IHRoaXMuY2xpZW50SWQ7XG4gICAgfVxuICAgIGlmICh0aGlzLmNsaWVudFNlY3JldCkge1xuICAgICAgX3BhcmFtcy5jbGllbnRfc2VjcmV0ID0gdGhpcy5jbGllbnRTZWNyZXQ7XG4gICAgfVxuICAgIGlmICh0aGlzLnJlZGlyZWN0VXJpKSB7XG4gICAgICBfcGFyYW1zLnJlZGlyZWN0X3VyaSA9IHRoaXMucmVkaXJlY3RVcmk7XG4gICAgfVxuICAgIGNvbnN0IHJldCA9IGF3YWl0IHRoaXMuX3Bvc3RQYXJhbXMoX3BhcmFtcyk7XG4gICAgcmV0dXJuIHJldCBhcyBUb2tlblJlc3BvbnNlO1xuICB9XG5cbiAgLyoqXG4gICAqIE9BdXRoMiBVc2VybmFtZS1QYXNzd29yZCBGbG93IChSZXNvdXJjZSBPd25lciBQYXNzd29yZCBDcmVkZW50aWFscylcbiAgICovXG4gIGFzeW5jIGF1dGhlbnRpY2F0ZShcbiAgICB1c2VybmFtZTogc3RyaW5nLFxuICAgIHBhc3N3b3JkOiBzdHJpbmcsXG4gICk6IFByb21pc2U8VG9rZW5SZXNwb25zZT4ge1xuICAgIGlmICghdGhpcy5jbGllbnRJZCB8fCAhdGhpcy5jbGllbnRTZWNyZXQpIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcignTm8gdmFsaWQgT0F1dGgyIGNsaWVudCBjb25maWd1cmF0aW9uIHNldCcpO1xuICAgIH1cbiAgICBjb25zdCByZXQgPSBhd2FpdCB0aGlzLl9wb3N0UGFyYW1zKHtcbiAgICAgIGdyYW50X3R5cGU6ICdwYXNzd29yZCcsXG4gICAgICB1c2VybmFtZSxcbiAgICAgIHBhc3N3b3JkLFxuICAgICAgY2xpZW50X2lkOiB0aGlzLmNsaWVudElkLFxuICAgICAgY2xpZW50X3NlY3JldDogdGhpcy5jbGllbnRTZWNyZXQsXG4gICAgfSk7XG4gICAgcmV0dXJuIHJldCBhcyBUb2tlblJlc3BvbnNlO1xuICB9XG5cbiAgLyoqXG4gICAqIE9BdXRoMiBSZXZva2UgU2Vzc2lvbiBUb2tlblxuICAgKi9cbiAgYXN5bmMgcmV2b2tlVG9rZW4odG9rZW46IHN0cmluZyk6IFByb21pc2U8dm9pZD4ge1xuICAgIGNvbnN0IHJlc3BvbnNlID0gYXdhaXQgdGhpcy5fdHJhbnNwb3J0Lmh0dHBSZXF1ZXN0KHtcbiAgICAgIG1ldGhvZDogJ1BPU1QnLFxuICAgICAgdXJsOiB0aGlzLnJldm9rZVNlcnZpY2VVcmwsXG4gICAgICBib2R5OiBxdWVyeXN0cmluZy5zdHJpbmdpZnkoeyB0b2tlbiB9KSxcbiAgICAgIGhlYWRlcnM6IHtcbiAgICAgICAgJ2NvbnRlbnQtdHlwZSc6ICdhcHBsaWNhdGlvbi94LXd3dy1mb3JtLXVybGVuY29kZWQnLFxuICAgICAgfSxcbiAgICB9KTtcbiAgICBpZiAocmVzcG9uc2Uuc3RhdHVzQ29kZSA+PSA0MDApIHtcbiAgICAgIGxldCByZXM6IGFueSA9IHF1ZXJ5c3RyaW5nLnBhcnNlKHJlc3BvbnNlLmJvZHkpO1xuICAgICAgaWYgKCFyZXMgfHwgIXJlcy5lcnJvcikge1xuICAgICAgICByZXMgPSB7XG4gICAgICAgICAgZXJyb3I6IGBFUlJPUl9IVFRQXyR7cmVzcG9uc2Uuc3RhdHVzQ29kZX1gLFxuICAgICAgICAgIGVycm9yX2Rlc2NyaXB0aW9uOiByZXNwb25zZS5ib2R5LFxuICAgICAgICB9O1xuICAgICAgfVxuICAgICAgdGhyb3cgbmV3IChjbGFzcyBleHRlbmRzIEVycm9yIHtcbiAgICAgICAgY29uc3RydWN0b3Ioe1xuICAgICAgICAgIGVycm9yLFxuICAgICAgICAgIGVycm9yX2Rlc2NyaXB0aW9uLFxuICAgICAgICB9OiB7XG4gICAgICAgICAgZXJyb3I6IHN0cmluZztcbiAgICAgICAgICBlcnJvcl9kZXNjcmlwdGlvbjogc3RyaW5nO1xuICAgICAgICB9KSB7XG4gICAgICAgICAgc3VwZXIoZXJyb3JfZGVzY3JpcHRpb24pO1xuICAgICAgICAgIHRoaXMubmFtZSA9IGVycm9yO1xuICAgICAgICB9XG4gICAgICB9KShyZXMpO1xuICAgIH1cbiAgfVxuXG4gIC8qKlxuICAgKiBAcHJpdmF0ZVxuICAgKi9cbiAgYXN5bmMgX3Bvc3RQYXJhbXMocGFyYW1zOiB7IFtuYW1lOiBzdHJpbmddOiBzdHJpbmcgfSk6IFByb21pc2U8YW55PiB7XG4gICAgaWYgKHRoaXMuY29kZVZlcmlmaWVyKSBwYXJhbXMuY29kZV92ZXJpZmllciA9IHRoaXMuY29kZVZlcmlmaWVyO1xuXG4gICAgY29uc3QgcmVzcG9uc2UgPSBhd2FpdCB0aGlzLl90cmFuc3BvcnQuaHR0cFJlcXVlc3Qoe1xuICAgICAgbWV0aG9kOiAnUE9TVCcsXG4gICAgICB1cmw6IHRoaXMudG9rZW5TZXJ2aWNlVXJsLFxuICAgICAgYm9keTogcXVlcnlzdHJpbmcuc3RyaW5naWZ5KHBhcmFtcyksXG4gICAgICBoZWFkZXJzOiB7XG4gICAgICAgICdjb250ZW50LXR5cGUnOiAnYXBwbGljYXRpb24veC13d3ctZm9ybS11cmxlbmNvZGVkJyxcbiAgICAgIH0sXG4gICAgfSk7XG4gICAgbGV0IHJlcztcbiAgICB0cnkge1xuICAgICAgcmVzID0gSlNPTi5wYXJzZShyZXNwb25zZS5ib2R5KTtcbiAgICB9IGNhdGNoIChlKSB7XG4gICAgICAvKiBlc2xpbnQtZGlzYWJsZSBuby1lbXB0eSAqL1xuICAgIH1cbiAgICBpZiAocmVzcG9uc2Uuc3RhdHVzQ29kZSA+PSA0MDApIHtcbiAgICAgIHJlcyA9IHJlcyB8fCB7XG4gICAgICAgIGVycm9yOiBgRVJST1JfSFRUUF8ke3Jlc3BvbnNlLnN0YXR1c0NvZGV9YCxcbiAgICAgICAgZXJyb3JfZGVzY3JpcHRpb246IHJlc3BvbnNlLmJvZHksXG4gICAgICB9O1xuICAgICAgdGhyb3cgbmV3IChjbGFzcyBleHRlbmRzIEVycm9yIHtcbiAgICAgICAgY29uc3RydWN0b3Ioe1xuICAgICAgICAgIGVycm9yLFxuICAgICAgICAgIGVycm9yX2Rlc2NyaXB0aW9uLFxuICAgICAgICB9OiB7XG4gICAgICAgICAgZXJyb3I6IHN0cmluZztcbiAgICAgICAgICBlcnJvcl9kZXNjcmlwdGlvbjogc3RyaW5nO1xuICAgICAgICB9KSB7XG4gICAgICAgICAgc3VwZXIoZXJyb3JfZGVzY3JpcHRpb24pO1xuICAgICAgICAgIHRoaXMubmFtZSA9IGVycm9yO1xuICAgICAgICB9XG4gICAgICB9KShyZXMpO1xuICAgIH1cbiAgICByZXR1cm4gcmVzO1xuICB9XG59XG5cbmV4cG9ydCBkZWZhdWx0IE9BdXRoMjtcbiJdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7OztBQUFBO0FBQ0E7QUFDQTtBQUNBLFNBQVFBLFVBQVUsRUFBRUMsV0FBVyxRQUFPLFFBQVE7QUFDOUMsT0FBT0MsV0FBVyxNQUFNLGFBQWE7QUFDckMsT0FBT0MsU0FBUyxJQUFHQyxrQkFBa0IsRUFBRUMsZ0JBQWdCLFFBQU8sYUFBYTtBQUczRSxJQUFNQyxtQkFBbUIsR0FBRztFQUMxQkMsUUFBUSxFQUFFO0FBQ1osQ0FBQzs7QUFFRDtBQUNBO0FBQ0EsU0FBU0MsZUFBZUEsQ0FBQ0MsYUFBcUIsRUFBVTtFQUN0RDtFQUNBO0VBQ0EsT0FBT0EsYUFBYSxDQUNqQkMsT0FBTyxDQUFDLEtBQUssRUFBRSxHQUFHLENBQUMsQ0FDbkJBLE9BQU8sQ0FBQyxLQUFLLEVBQUUsR0FBRyxDQUFDLENBQ25CQSxPQUFPLENBQUMsSUFBSSxFQUFFLEVBQUUsQ0FBQztBQUN0Qjs7QUFFQTtBQUNBO0FBQ0E7O0FBdUNBO0FBQ0E7QUFDQTtBQUNBLFdBQWFDLE1BQU07RUFZakI7QUFDRjtBQUNBO0VBQ0UsU0FBQUEsT0FBWUMsTUFBb0IsRUFB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