import _toConsumableArray from "@babel/runtime-corejs3/helpers/toConsumableArray";
import _asyncToGenerator from "@babel/runtime-corejs3/helpers/asyncToGenerator";
import _classCallCheck from "@babel/runtime-corejs3/helpers/classCallCheck";
import _createClass from "@babel/runtime-corejs3/helpers/createClass";
import _defineProperty from "@babel/runtime-corejs3/helpers/defineProperty";
import _regeneratorRuntime from "@babel/runtime-corejs3/regenerator";
import "core-js/modules/es.error.cause.js";
import "core-js/modules/es.array.join.js";
import "core-js/modules/es.function.name.js";
import "core-js/modules/es.object.keys.js";
import "core-js/modules/es.object.to-string.js";
import "core-js/modules/es.regexp.to-string.js";
import _concatInstanceProperty from "@babel/runtime-corejs3/core-js-stable/instance/concat";
import _mapInstanceProperty from "@babel/runtime-corejs3/core-js-stable/instance/map";
import _Object$keys from "@babel/runtime-corejs3/core-js-stable/object/keys";
import _Promise from "@babel/runtime-corejs3/core-js-stable/promise";
import _filterInstanceProperty from "@babel/runtime-corejs3/core-js-stable/instance/filter";
import { exec } from 'child_process';
import stripAnsi from 'strip-ansi';
import Connection from '../connection';
function isNotNullOrUndefined(v) {
  return v != null;
}

/**
 *
 */
export var SfdxRegistry = /*#__PURE__*/function () {
  function SfdxRegistry(_ref) {
    var cliPath = _ref.cliPath;
    _classCallCheck(this, SfdxRegistry);
    _defineProperty(this, "_orgInfoMap", {});
    this._cliPath = cliPath;
  }
  return _createClass(SfdxRegistry, [{
    key: "_createCommand",
    value: function _createCommand(command) {
      var _context, _context2, _context3, _context4;
      var options = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : {};
      var args = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : [];
      return _concatInstanceProperty(_context = _concatInstanceProperty(_context2 = _concatInstanceProperty(_context3 = "".concat(this._cliPath ? this._cliPath + '/' : '', "sfdx ")).call(_context3, command, " ")).call(_context2, _mapInstanceProperty(_context4 = _Object$keys(options)).call(_context4, function (option) {
        var _context5, _context6;
        return _concatInstanceProperty(_context5 = _concatInstanceProperty(_context6 = "".concat(option.length > 1 ? '--' : '-')).call(_context6, option)).call(_context5, options[option] != null ? ' ' + options[option] : '');
      }).join(' '), " --json ")).call(_context, args.join(' '));
    }
  }, {
    key: "_execCommand",
    value: function () {
      var _execCommand2 = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee(command) {
        var options,
          args,
          cmd,
          buf,
          body,
          ret,
          err,
          _args = arguments;
        return _regeneratorRuntime.wrap(function _callee$(_context7) {
          while (1) switch (_context7.prev = _context7.next) {
            case 0:
              options = _args.length > 1 && _args[1] !== undefined ? _args[1] : {};
              args = _args.length > 2 && _args[2] !== undefined ? _args[2] : [];
              cmd = this._createCommand(command, options, args);
              _context7.next = 5;
              return new _Promise(function (resolve, reject) {
                exec(cmd, function (err, ret) {
                  if (err && !ret) {
                    reject(err);
                  } else {
                    resolve(ret);
                  }
                });
              });
            case 5:
              buf = _context7.sent;
              body = stripAnsi(buf.toString());
              _context7.prev = 7;
              ret = JSON.parse(body);
              _context7.next = 14;
              break;
            case 11:
              _context7.prev = 11;
              _context7.t0 = _context7["catch"](7);
              throw new Error("Unexpectedd output from Sfdx cli: ".concat(body));
            case 14:
              if (!(ret.status === 0 && ret.result)) {
                _context7.next = 18;
                break;
              }
              return _context7.abrupt("return", ret.result);
            case 18:
              err = new Error(ret.message);
              err.name = ret.name;
              throw err;
            case 21:
            case "end":
              return _context7.stop();
          }
        }, _callee, this, [[7, 11]]);
      }));
      function _execCommand(_x) {
        return _execCommand2.apply(this, arguments);
      }
      return _execCommand;
    }()
  }, {
    key: "_getOrgList",
    value: function () {
      var _getOrgList2 = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee2() {
        return _regeneratorRuntime.wrap(function _callee2$(_context8) {
          while (1) switch (_context8.prev = _context8.next) {
            case 0:
              if (!this._orgList) {
                this._orgList = this._execCommand('force:org:list');
              }
              return _context8.abrupt("return", this._orgList);
            case 2:
            case "end":
              return _context8.stop();
          }
        }, _callee2, this);
      }));
      function _getOrgList() {
        return _getOrgList2.apply(this, arguments);
      }
      return _getOrgList;
    }()
  }, {
    key: "getConnectionNames",
    value: function () {
      var _getConnectionNames = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee3() {
        var _context9, _context10, _context11;
        var _yield$this$_getOrgLi, nonScratchOrgs, scratchOrgs;
        return _regeneratorRuntime.wrap(function _callee3$(_context12) {
          while (1) switch (_context12.prev = _context12.next) {
            case 0:
              _context12.next = 2;
              return this._getOrgList();
            case 2:
              _yield$this$_getOrgLi = _context12.sent;
              nonScratchOrgs = _yield$this$_getOrgLi.nonScratchOrgs;
              scratchOrgs = _yield$this$_getOrgLi.scratchOrgs;
              return _context12.abrupt("return", _concatInstanceProperty(_context9 = []).call(_context9, _toConsumableArray(_filterInstanceProperty(_context10 = _mapInstanceProperty(nonScratchOrgs).call(nonScratchOrgs, function (o) {
                return o.alias;
              })).call(_context10, isNotNullOrUndefined)), _toConsumableArray(_filterInstanceProperty(_context11 = _mapInstanceProperty(scratchOrgs).call(scratchOrgs, function (o) {
                return o.alias;
              })).call(_context11, isNotNullOrUndefined)), _toConsumableArray(_mapInstanceProperty(nonScratchOrgs).call(nonScratchOrgs, function (o) {
                return o.username;
              })), _toConsumableArray(_mapInstanceProperty(scratchOrgs).call(scratchOrgs, function (o) {
                return o.username;
              }))));
            case 6:
            case "end":
              return _context12.stop();
          }
        }, _callee3, this);
      }));
      function getConnectionNames() {
        return _getConnectionNames.apply(this, arguments);
      }
      return getConnectionNames;
    }()
  }, {
    key: "getConnection",
    value: function () {
      var _getConnection = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee4(name) {
        var config;
        return _regeneratorRuntime.wrap(function _callee4$(_context13) {
          while (1) switch (_context13.prev = _context13.next) {
            case 0:
              _context13.next = 2;
              return this.getConnectionConfig(name);
            case 2:
              config = _context13.sent;
              return _context13.abrupt("return", config ? new Connection(config) : null);
            case 4:
            case "end":
              return _context13.stop();
          }
        }, _callee4, this);
      }));
      function getConnection(_x2) {
        return _getConnection.apply(this, arguments);
      }
      return getConnection;
    }()
  }, {
    key: "_getOrgInfo",
    value: function () {
      var _getOrgInfo2 = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee5(username) {
        var options, pOrgInfo, orgInfo;
        return _regeneratorRuntime.wrap(function _callee5$(_context14) {
          while (1) switch (_context14.prev = _context14.next) {
            case 0:
              options = username ? {
                u: username
              } : {};
              if (!username || !this._orgInfoMap[username]) {
                pOrgInfo = this._execCommand('force:org:display', options);
                this._memoOrgInfo(pOrgInfo, username);
              }
              orgInfo = username ? this._orgInfoMap[username] : this._defaultOrgInfo;
              if (orgInfo) {
                _context14.next = 5;
                break;
              }
              throw new Error('no orginfo found');
            case 5:
              return _context14.abrupt("return", orgInfo);
            case 6:
            case "end":
              return _context14.stop();
          }
        }, _callee5, this);
      }));
      function _getOrgInfo(_x3) {
        return _getOrgInfo2.apply(this, arguments);
      }
      return _getOrgInfo;
    }()
  }, {
    key: "_memoOrgInfo",
    value: function _memoOrgInfo(pOrgInfo, username) {
      var _this = this;
      var pOrgInfo_ = pOrgInfo.then(function (orgInfo) {
        _this._orgInfoMap[orgInfo.username] = pOrgInfo_;
        if (orgInfo.alias) {
          _this._orgInfoMap[orgInfo.alias] = pOrgInfo_;
        }
        return orgInfo;
      });
      if (username) {
        this._orgInfoMap[username] = pOrgInfo_;
      } else {
        this._defaultOrgInfo = pOrgInfo_;
      }
    }
  }, {
    key: "getConnectionConfig",
    value: function () {
      var _getConnectionConfig = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee6(name) {
        var orgInfo, accessToken, instanceUrl, loginUrl;
        return _regeneratorRuntime.wrap(function _callee6$(_context15) {
          while (1) switch (_context15.prev = _context15.next) {
            case 0:
              _context15.next = 2;
              return this._getOrgInfo(name);
            case 2:
              orgInfo = _context15.sent;
              if (orgInfo) {
                _context15.next = 5;
                break;
              }
              return _context15.abrupt("return", null);
            case 5:
              accessToken = orgInfo.accessToken, instanceUrl = orgInfo.instanceUrl, loginUrl = orgInfo.loginUrl;
              return _context15.abrupt("return", {
                accessToken: accessToken,
                instanceUrl: instanceUrl,
                loginUrl: loginUrl
              });
            case 7:
            case "end":
              return _context15.stop();
          }
        }, _callee6, this);
      }));
      function getConnectionConfig(_x4) {
        return _getConnectionConfig.apply(this, arguments);
      }
      return getConnectionConfig;
    }()
  }, {
    key: "saveConnectionConfig",
    value: function () {
      var _saveConnectionConfig = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee7(_name, _connConfig) {
        return _regeneratorRuntime.wrap(function _callee7$(_context16) {
          while (1) switch (_context16.prev = _context16.next) {
            case 0:
            case "end":
              return _context16.stop();
          }
        }, _callee7);
      }));
      function saveConnectionConfig(_x5, _x6) {
        return _saveConnectionConfig.apply(this, arguments);
      }
      return saveConnectionConfig;
    }()
  }, {
    key: "setDefaultConnection",
    value: function () {
      var _setDefaultConnection = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee8(_name) {
        return _regeneratorRuntime.wrap(function _callee8$(_context17) {
          while (1) switch (_context17.prev = _context17.next) {
            case 0:
            case "end":
              return _context17.stop();
          }
        }, _callee8);
      }));
      function setDefaultConnection(_x7) {
        return _setDefaultConnection.apply(this, arguments);
      }
      return setDefaultConnection;
    }()
  }, {
    key: "removeConnectionConfig",
    value: function () {
      var _removeConnectionConfig = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee9(name) {
        return _regeneratorRuntime.wrap(function _callee9$(_context18) {
          while (1) switch (_context18.prev = _context18.next) {
            case 0:
              _context18.next = 2;
              return this._execCommand('force:org:delete', {
                u: name
              });
            case 2:
            case "end":
              return _context18.stop();
          }
        }, _callee9, this);
      }));
      function removeConnectionConfig(_x8) {
        return _removeConnectionConfig.apply(this, arguments);
      }
      return removeConnectionConfig;
    }() // eslint-disable-next-line @typescript-eslint/require-await
  }, {
    key: "getClientConfig",
    value: function () {
      var _getClientConfig = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee10(_name) {
        return _regeneratorRuntime.wrap(function _callee10$(_context19) {
          while (1) switch (_context19.prev = _context19.next) {
            case 0:
              return _context19.abrupt("return", null);
            case 1:
            case "end":
              return _context19.stop();
          }
        }, _callee10);
      }));
      function getClientConfig(_x9) {
        return _getClientConfig.apply(this, arguments);
      }
      return getClientConfig;
    }() // eslint-disable-next-line @typescript-eslint/require-await
  }, {
    key: "getClientNames",
    value: function () {
      var _getClientNames = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee11() {
        return _regeneratorRuntime.wrap(function _callee11$(_context20) {
          while (1) switch (_context20.prev = _context20.next) {
            case 0:
              return _context20.abrupt("return", []);
            case 1:
            case "end":
              return _context20.stop();
          }
        }, _callee11);
      }));
      function getClientNames() {
        return _getClientNames.apply(this, arguments);
      }
      return getClientNames;
    }()
  }, {
    key: "registerClientConfig",
    value: function () {
      var _registerClientConfig = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee12(_name, _clientConfig) {
        return _regeneratorRuntime.wrap(function _callee12$(_context21) {
          while (1) switch (_context21.prev = _context21.next) {
            case 0:
            case "end":
              return _context21.stop();
          }
        }, _callee12);
      }));
      function registerClientConfig(_x10, _x11) {
        return _registerClientConfig.apply(this, arguments);
      }
      return registerClientConfig;
    }()
  }]);
}();
//# sourceMappingURL=data:application/json;charset=utf-8;base64,eyJ2ZXJzaW9uIjozLCJuYW1lcyI6WyJleGVjIiwic3RyaXBBbnNpIiwiQ29ubmVjdGlvbiIsImlzTm90TnVsbE9yVW5kZWZpbmVkIiwidiIsIlNmZHhSZWdpc3RyeSIsIl9yZWYiLCJjbGlQYXRoIiwiX2NsYXNzQ2FsbENoZWNrIiwiX2RlZmluZVByb3BlcnR5IiwiX2NsaVBhdGgiLCJfY3JlYXRlQ2xhc3MiLCJrZXkiLCJ2YWx1ZSIsIl9jcmVhdGVDb21tYW5kIiwiY29tbWFuZCIsIl9jb250ZXh0IiwiX2NvbnRleHQyIiwiX2NvbnRleHQzIiwiX2NvbnRleHQ0Iiwib3B0aW9ucyIsImFyZ3VtZW50cyIsImxlbmd0aCIsInVuZGVmaW5lZCIsImFyZ3MiLCJfY29uY2F0SW5zdGFuY2VQcm9wZXJ0eSIsImNvbmNhdCIsImNhbGwiLCJfbWFwSW5zdGFuY2VQcm9wZXJ0eSIsIl9PYmplY3Qka2V5cyIsIm9wdGlvbiIsIl9jb250ZXh0NSIsIl9jb250ZXh0NiIsImpvaW4iLCJfZXhlY0NvbW1hbmQyIiwiX2FzeW5jVG9HZW5lcmF0b3IiLCJfcmVnZW5lcmF0b3JSdW50aW1lIiwibWFyayIsIl9jYWxsZWUiLCJjbWQiLCJidWYiLCJib2R5IiwicmV0IiwiZXJyIiwiX2FyZ3MiLCJ3cmFwIiwiX2NhbGxlZSQiLCJfY29udGV4dDciLCJwcmV2IiwibmV4dCIsIl9Qcm9taXNlIiwicmVzb2x2ZSIsInJlamVjdCIsInNlbnQiLCJ0b1N0cmluZyIsIkpTT04iLCJwYXJzZSIsInQwIiwiRXJyb3IiLCJzdGF0dXMiLCJyZXN1bHQiLCJhYnJ1cHQiLCJtZXNzYWdlIiwibmFtZSIsInN0b3AiLCJfZXhlY0NvbW1hbmQiLCJfeCIsImFwcGx5IiwiX2dldE9yZ0xpc3QyIiwiX2NhbGxlZTIiLCJfY2FsbGVlMiQiLCJfY29udGV4dDgiLCJfb3JnTGlzdCIsIl9nZXRPcmdMaXN0IiwiX2dldENvbm5lY3Rpb25OYW1lcyIsIl9jYWxsZWUzIiwiX2NvbnRleHQ5IiwiX2NvbnRleHQxMCIsIl9jb250ZXh0MTEiLCJfeWllbGQkdGhpcyRfZ2V0T3JnTGkiLCJub25TY3JhdGNoT3JncyIsInNjcmF0Y2hPcmdzIiwiX2NhbGxlZTMkIiwiX2NvbnRleHQxMiIsIl90b0NvbnN1bWFibGVBcnJheSIsIl9maWx0ZXJJbnN0YW5jZVByb3BlcnR5IiwibyIsImFsaWFzIiwidXNlcm5hbWUiLCJnZXRDb25uZWN0aW9uTmFtZXMiLCJfZ2V0Q29ubmVjdGlvbiIsIl9jYWxsZWU0IiwiY29uZmlnIiwiX2NhbGxlZTQkIiwiX2NvbnRleHQxMyIsImdldENvbm5lY3Rpb25Db25maWciLCJnZXRDb25uZWN0aW9uIiwiX3gyIiwiX2dldE9yZ0luZm8yIiwiX2NhbGxlZTUiLCJwT3JnSW5mbyIsIm9yZ0luZm8iLCJfY2FsbGVlNSQiLCJfY29udGV4dDE0IiwidSIsIl9vcmdJbmZvTWFwIiwiX21lbW9PcmdJbmZvIiwiX2RlZmF1bHRPcmdJbmZvIiwiX2dldE9yZ0luZm8iLCJfeDMiLCJfdGhpcyIsInBPcmdJbmZvXyIsInRoZW4iLCJfZ2V0Q29ubmVjdGlvbkNvbmZpZyIsIl9jYWxsZWU2IiwiYWNjZXNzVG9rZW4iLCJpbnN0YW5jZVVybCIsImxvZ2luVXJsIiwiX2NhbGxlZTYkIiwiX2NvbnRleHQxNSIsIl94NCIsIl9zYXZlQ29ubmVjdGlvbkNvbmZpZyIsIl9jYWxsZWU3IiwiX25hbWUiLCJfY29ubkNvbmZpZyIsIl9jYWxsZWU3JCIsIl9jb250ZXh0MTYiLCJzYXZlQ29ubmVjdGlvbkNvbmZpZyIsIl94NSIsIl94NiIsIl9zZXREZWZhdWx0Q29ubmVjdGlvbiIsIl9jYWxsZWU4IiwiX2NhbGxlZTgkIiwiX2NvbnRleHQxNyIsInNldERlZmF1bHRDb25uZWN0aW9uIiwiX3g3IiwiX3JlbW92ZUNvbm5lY3Rpb25Db25maWciLCJfY2FsbGVlOSIsIl9jYWxsZWU5JCIsIl9jb250ZXh0MTgiLCJyZW1vdmVDb25uZWN0aW9uQ29uZmlnIiwiX3g4IiwiX2dldENsaWVudENvbmZpZyIsIl9jYWxsZWUxMCIsIl9jYWxsZWUxMCQiLCJfY29udGV4dDE5IiwiZ2V0Q2xpZW50Q29uZmlnIiwiX3g5IiwiX2dldENsaWVudE5hbWVzIiwiX2NhbGxlZTExIiwiX2NhbGxlZTExJCIsIl9jb250ZXh0MjAiLCJnZXRDbGllbnROYW1lcyIsIl9yZWdpc3RlckNsaWVudENvbmZpZyIsIl9jYWxsZWUxMiIsIl9jbGllbnRDb25maWciLCJfY2FsbGVlMTIkIiwiX2NvbnRleHQyMSIsInJlZ2lzdGVyQ2xpZW50Q29uZmlnIiwiX3gxMCIsIl94MTEiXSwic291cmNlcyI6WyIuLi8uLi9zcmMvcmVnaXN0cnkvc2ZkeC50cyJdLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQgeyBleGVjIH0gZnJvbSAnY2hpbGRfcHJvY2Vzcyc7XG5pbXBvcnQgc3RyaXBBbnNpIGZyb20gJ3N0cmlwLWFuc2knO1xuaW1wb3J0IENvbm5lY3Rpb24gZnJvbSAnLi4vY29ubmVjdGlvbic7XG5pbXBvcnQgeyBSZWdpc3RyeSwgQ29ubmVjdGlvbkNvbmZpZywgQ2xpZW50Q29uZmlnIH0gZnJvbSAnLi90eXBlcyc7XG5pbXBvcnQgeyBTY2hlbWEgfSBmcm9tICcuLi90eXBlcyc7XG5cbnR5cGUgU2ZkeENvbW1hbmRPdXRwdXQgPSB7XG4gIHN0YXR1czogbnVtYmVyO1xuICBuYW1lPzogc3RyaW5nO1xuICBtZXNzYWdlPzogc3RyaW5nO1xuICByZXN1bHQ/OiBhbnk7XG59O1xuXG50eXBlIFNmZHhPcmdMaXN0ID0ge1xuICBub25TY3JhdGNoT3JnczogU2ZkeE9yZ0luZm9bXTtcbiAgc2NyYXRjaE9yZ3M6IFNmZHhPcmdJbmZvW107XG59O1xuXG50eXBlIFNmZHhPcmdJbmZvID0ge1xuICBvcmdJZDogc3RyaW5nO1xuICBhY2Nlc3NUb2tlbjogc3RyaW5nO1xuICBpbnN0YW5jZVVybDogc3RyaW5nO1xuICBsb2dpblVybDogc3RyaW5nO1xuICB1c2VybmFtZTogc3RyaW5nO1xuICBjbGllbnRJZDogc3RyaW5nO1xuICBpc0Rldkh1YjogYm9vbGVhbjtcbiAgY29ubmVjdGVkU3RhdHVzOiBzdHJpbmc7XG4gIGxhc3RVc2VkOiBzdHJpbmc7XG4gIGFsaWFzPzogc3RyaW5nO1xufTtcblxuZnVuY3Rpb24gaXNOb3ROdWxsT3JVbmRlZmluZWQ8VD4odjogVCB8IG51bGwgfCB1bmRlZmluZWQpOiB2IGlzIFQge1xuICByZXR1cm4gdiAhPSBudWxsO1xufVxuXG4vKipcbiAqXG4gKi9cbmV4cG9ydCBjbGFzcyBTZmR4UmVnaXN0cnkgaW1wbGVtZW50cyBSZWdpc3RyeSB7XG4gIF9jbGlQYXRoOiBzdHJpbmcgfCB1bmRlZmluZWQ7XG4gIF9vcmdMaXN0OiBQcm9taXNlPFNmZHhPcmdMaXN0PiB8IHVuZGVmaW5lZDtcbiAgX29yZ0luZm9NYXA6IHsgW25hbWU6IHN0cmluZ106IFByb21pc2U8U2ZkeE9yZ0luZm8+IH0gPSB7fTtcbiAgX2RlZmF1bHRPcmdJbmZvOiBQcm9taXNlPFNmZHhPcmdJbmZvPiB8IHVuZGVmaW5lZDtcblxuICBjb25zdHJ1Y3Rvcih7IGNsaVBhdGggfTogeyBjbGlQYXRoPzogc3RyaW5nIH0pIHtcbiAgICB0aGlzLl9jbGlQYXRoID0gY2xpUGF0aDtcbiAgfVxuXG4gIF9jcmVhdGVDb21tYW5kKFxuICAgIGNvbW1hbmQ6IHN0cmluZyxcbiAgICBvcHRpb25zOiB7IFtvcHRpb246IHN0cmluZ106IGFueSB9ID0ge30sXG4gICAgYXJnczogc3RyaW5nW10gPSBbXSxcbiAgKSB7XG4gICAgcmV0dXJuIGAke1xuICAgICAgdGhpcy5fY2xpUGF0aCA/IHRoaXMuX2NsaVBhdGggKyAnLycgOiAnJ1xuICAgIH1zZmR4ICR7Y29tbWFuZH0gJHtPYmplY3Qua2V5cyhvcHRpb25zKVxuICAgICAgLm1hcChcbiAgICAgICAgKG9wdGlvbikgPT5cbiAgICAgICAgICBgJHtvcHRpb24ubGVuZ3RoID4gMSA/ICctLScgOiAnLSd9JHtvcHRpb259JHtcbiAgICAgICAgICAgIG9wdGlvbnNbb3B0aW9uXSAhPSBudWxsID8gJyAnICsgb3B0aW9uc1tvcHRpb25dIDogJydcbiAgICAgICAgICB9YCxcbiAgICAgIClcbiAgICAgIC5qb2luKCcgJyl9IC0tanNvbiAke2FyZ3Muam9pbignICcpfWA7XG4gIH1cblxuICBhc3luYyBfZXhlY0NvbW1hbmQ8VD4oXG4gICAgY29tbWFuZDogc3RyaW5nLFxuICAgIG9wdGlvbnM6IHsgW29wdGlvbjogc3RyaW5nXTogYW55IH0gPSB7fSxcbiAgICBhcmdzOiBzdHJpbmdbXSA9IFtdLFxuICApIHtcbiAgICBjb25zdCBjbWQgPSB0aGlzLl9jcmVhdGVDb21tYW5kKGNvbW1hbmQsIG9wdGlvbnMsIGFyZ3MpO1xuICAgIGNvbnN0IGJ1ZiA9IGF3YWl0IG5ldyBQcm9taXNlPHN0cmluZz4oKHJlc29sdmUsIHJlamVjdCkgPT4ge1xuICAgICAgZXhlYyhjbWQsIChlcnIsIHJldCkgPT4ge1xuICAgICAgICBpZiAoZXJyICYmICFyZXQpIHtcbiAgICAgICAgICByZWplY3QoZXJyKTtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICByZXNvbHZlKHJldCk7XG4gICAgICAgIH1cbiAgICAgIH0pO1xuICAgIH0pO1xuICAgIGNvbnN0IGJvZHkgPSBzdHJpcEFuc2koYnVmLnRvU3RyaW5nKCkpO1xuICAgIGxldCByZXQ6IFNmZHhDb21tYW5kT3V0cHV0O1xuICAgIHRyeSB7XG4gICAgICByZXQgPSBKU09OLnBhcnNlKGJvZHkpIGFzIFNmZHhDb21tYW5kT3V0cHV0O1xuICAgIH0gY2F0Y2ggKGUpIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcihgVW5leHBlY3RlZGQgb3V0cHV0IGZyb20gU2ZkeCBjbGk6ICR7Ym9keX1gKTtcbiAgICB9XG4gICAgaWYgKHJldC5zdGF0dXMgPT09IDAgJiYgcmV0LnJlc3VsdCkge1xuICAgICAgcmV0dXJuIHJldC5yZXN1bHQgYXMgVDtcbiAgICB9IGVsc2Uge1xuICAgICAgY29uc3QgZXJyID0gbmV3IEVycm9yKHJldC5tZXNzYWdlIGFzIHN0cmluZyk7XG4gICAgICBlcnIubmFtZSA9IHJldC5uYW1lIGFzIHN0cmluZztcbiAgICAgIHRocm93IGVycjtcbiAgICB9XG4gIH1cblxuICBhc3luYyBfZ2V0T3JnTGlzdCgpIHtcbiAgICBpZiAoIXRoaXMuX29yZ0xpc3QpIHtcbiAgICAgIHRoaXMuX29yZ0xpc3QgPSB0aGlzLl9leGVjQ29tbWFuZDxTZmR4T3JnTGlzdD4oJ2ZvcmNlOm9yZzpsaXN0Jyk7XG4gICAgfVxuICAgIHJldHVybiB0aGlzLl9vcmdMaXN0O1xuICB9XG5cbiAgYXN5bmMgZ2V0Q29ubmVjdGlvbk5hbWVzKCkge1xuICAgIGNvbnN0IHsgbm9uU2NyYXRjaE9yZ3MsIHNjcmF0Y2hPcmdzIH0gPSBhd2FpdCB0aGlzLl9nZXRPcmdMaXN0KCk7XG4gICAgcmV0dXJuIFtcbiAgICAgIC4uLm5vblNjcmF0Y2hPcmdzLm1hcCgobykgPT4gby5hbGlhcykuZmlsdGVyKGlzTm90TnVsbE9yVW5kZWZpbmVkKSxcbiAgICAgIC4uLnNjcmF0Y2hPcmdzLm1hcCgobykgPT4gby5hbGlhcykuZmlsdGVyKGlzTm90TnVsbE9yVW5kZWZpbmVkKSxcbiAgICAgIC4uLm5vblNjcmF0Y2hPcmdzLm1hcCgobykgPT4gby51c2VybmFtZSksXG4gICAgICAuLi5zY3JhdGNoT3Jncy5tYXAoKG8pID0+IG8udXNlcm5hbWUpLFxuICAgIF07XG4gIH1cblxuICBhc3luYyBnZXRDb25uZWN0aW9uPFMgZXh0ZW5kcyBTY2hlbWEgPSBTY2hlbWE+KG5hbWU/OiBzdHJpbmcpIHtcbiAgICBjb25zdCBjb25maWcgPSBhd2FpdCB0aGlzLmdldENvbm5lY3Rpb25Db25maWcobmFtZSk7XG4gICAgcmV0dXJuIGNvbmZpZyA/IG5ldyBDb25uZWN0aW9uPFM+KGNvbmZpZykgOiBudWxsO1xuICB9XG5cbiAgYXN5bmMgX2dldE9yZ0luZm8odXNlcm5hbWU/OiBzdHJpbmcpIHtcbiAgICBjb25zdCBvcHRpb25zID0gdXNlcm5hbWUgPyB7IHU6IHVzZXJuYW1lIH0gOiB7fTtcbiAgICBpZiAoIXVzZXJuYW1lIHx8ICF0aGlzLl9vcmdJbmZvTWFwW3VzZXJuYW1lXSkge1xuICAgICAgY29uc3QgcE9yZ0luZm8gPSB0aGlzLl9leGVjQ29tbWFuZDxTZmR4T3JnSW5mbz4oXG4gICAgICAgICdmb3JjZTpvcmc6ZGlzcGxheScsXG4gICAgICAgIG9wdGlvbnMsXG4gICAgICApO1xuICAgICAgdGhpcy5fbWVtb09yZ0luZm8ocE9yZ0luZm8sIHVzZXJuYW1lKTtcbiAgICB9XG4gICAgY29uc3Qgb3JnSW5mbyA9IHVzZXJuYW1lXG4gICAgICA/IHRoaXMuX29yZ0luZm9NYXBbdXNlcm5hbWVdXG4gICAgICA6IHRoaXMuX2RlZmF1bHRPcmdJbmZvO1xuICAgIGlmICghb3JnSW5mbykge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKCdubyBvcmdpbmZvIGZvdW5kJyk7XG4gICAgfVxuICAgIHJldHVybiBvcmdJbmZvO1xuICB9XG5cbiAgX21lbW9PcmdJbmZvKHBPcmdJbmZvOiBQcm9taXNlPFNmZHhPcmdJbmZvPiwgdXNlcm5hbWU/OiBzdHJpbmcpIHtcbiAgICBjb25zdCBwT3JnSW5mb18gPSBwT3JnSW5mby50aGVuKChvcmdJbmZvKSA9PiB7XG4gICAgICB0aGlzLl9vcmdJbmZvTWFwW29yZ0luZm8udXNlcm5hbWVdID0gcE9yZ0luZm9fO1xuICAgICAgaWYgKG9yZ0luZm8uYWxpYXMpIHtcbiAgICAgICAgdGhpcy5fb3JnSW5mb01hcFtvcmdJbmZvLmFsaWFzXSA9IHBPcmdJbmZvXztcbiAgICAgIH1cbiAgICAgIHJldHVybiBvcmdJbmZvO1xuICAgIH0pO1xuICAgIGlmICh1c2VybmFtZSkge1xuICAgICAgdGhpcy5fb3JnSW5mb01hcFt1c2VybmFtZV0gPSBwT3JnSW5mb187XG4gICAgfSBlbHNlIHtcbiAgICAgIHRoaXMuX2RlZmF1bHRPcmdJbmZvID0gcE9yZ0luZm9fO1xuICAgIH1cbiAgfVxuXG4gIGFzeW5jIGdldENvbm5lY3Rpb25Db25maWcobmFtZT86IHN0cmluZykge1xuICAgIGNvbnN0IG9yZ0luZm8gPSBhd2FpdCB0aGlzLl9nZXRPcmdJbmZvKG5hbWUpO1xuICAgIGlmICghb3JnSW5mbykge1xuICAgICAgcmV0dXJuIG51bGw7XG4gICAgfVxuICAgIGNvbnN0IHsgYWNjZXNzVG9rZW4sIGluc3RhbmNlVXJsLCBsb2dpblVybCB9ID0gb3JnSW5mbztcbiAgICByZXR1cm4geyBhY2Nlc3NUb2tlbiwgaW5zdGFuY2VVcmwsIGxvZ2luVXJsIH07XG4gIH1cblxuICBhc3luYyBzYXZlQ29ubmVjdGlvbkNvbmZpZyhfbmFtZTogc3RyaW5nLCBfY29ubkNvbmZpZzogQ29ubmVjdGlvbkNvbmZpZykge1xuICAgIC8vIG5vdGhpbmcgdG8gZG9cbiAgfVxuXG4gIGFzeW5jIHNldERlZmF1bHRDb25uZWN0aW9uKF9uYW1lOiBzdHJpbmcpIHtcbiAgICAvLyBub3RoaW5nIHRvIGRvXG4gIH1cblxuICBhc3luYyByZW1vdmVDb25uZWN0aW9uQ29uZmlnKG5hbWU6IHN0cmluZykge1xuICAgIGF3YWl0IHRoaXMuX2V4ZWNDb21tYW5kKCdmb3JjZTpvcmc6ZGVsZXRlJywgeyB1OiBuYW1lIH0pO1xuICB9XG4gIC8vIGVzbGludC1kaXNhYmxlLW5leHQtbGluZSBAdHlwZXNjcmlwdC1lc2xpbnQvcmVxdWlyZS1hd2FpdFxuICBhc3luYyBnZXRDbGllbnRDb25maWcoX25hbWU6IHN0cmluZykge1xuICAgIHJldHVybiBudWxsO1xuICB9XG5cbiAgLy8gZXNsaW50LWRpc2FibGUtbmV4dC1saW5lIEB0eXBlc2NyaXB0LWVzbGludC9yZXF1aXJlLWF3YWl0XG4gIGFzeW5jIGdldENsaWVudE5hbWVzKCkge1xuICAgIHJldHVybiBbXTtcbiAgfVxuXG4gIGFzeW5jIHJlZ2lzdGVyQ2xpZW50Q29uZmlnKF9uYW1lOiBzdHJpbmcsIF9jbGllbnRDb25maWc6IENsaWVudENvbmZpZykge1xuICAgIC8vIG5vdGhpbmcgdG8gZG9cbiAgfVxufVxuIl0sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7Ozs7Ozs7OztBQUFBLFNBQVNBLElBQUksUUFBUSxlQUFlO0FBQ3BDLE9BQU9DLFNBQVMsTUFBTSxZQUFZO0FBQ2xDLE9B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