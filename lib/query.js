'use strict';

var events = require('events');
var util = require('util');
var _ = require('lodash');
var flow = require('ent-flow');
var $ = require('highland');
var P = require('bluebird');
var Logger = require('glib').Logger;
var log = new Logger('query');
var redis = require('redis');
P.promisifyAll(redis.RedisClient.prototype);
P.promisifyAll(redis.Multi.prototype);

/**
 * @param {Query} query
 * @param {Object} data
 * @constructor
 */
function QueryResult(query, data) {
    this.query = query;
    this.cancelled = false;
    this.fields = {};
    this.selected = -1;
    this._through = query._through.slice();
    this.query.on('cancel', this.onCancel.bind(this));
    this.data = this._initDataStream(data);
}

QueryResult.prototype._initDataStream = function(source) {
    source = source || [];

    return this.query.limit >= 0 ? $ (source).take(this.query.limit) : $(source);
};

/**
 * Shuts down the stream when the query has been cancelled
 */
QueryResult.prototype.onCancel = function() {
    this.data.destroy();
};

/**
 * Forwards cancellations to the query
 * @return {QueryResult}
 */
QueryResult.prototype.cancel = function() {
    this.query.cancel();
    return this;
};

/**
 *
 * @return {bluebird|exports|module.exports}
 */
QueryResult.prototype.toArray = function() {
    var self = this;

    return new P(function(resolve) {
        self.stream().toArray(resolve);
    })
};

QueryResult.prototype._cacheStream = function(stream) {

};

QueryResult.prototype.stream = function() {
    try {
        var streams = []
            .concat(_.identity.bind(null, this.data))
            .concat(this._through)
            .concat(this._cacheStream.bind(this));

        return flow.create(this, streams).stream();
    } catch (err) {
        log.error('Error assembling flow, cancelling: ' + err.message);
        this.query.cancel();
        throw err;
    }
};

/**
 * Creates a shim for use in the reply to set properties on the result
 * @return {{fields: Function, meta: Function}}
 */
QueryResult.prototype.shim = function() {
    var self = this;

    return {
        fields: function (value) {
            value = _.isArray(value) ? _.object(value.map(v => [v, {}])) : value;
            self.fields = _.merge(self.fields, value);
            return this;
        },
        meta: function (value) {
            self.meta = value;
            return this;
        },
        selected: function (value) {
            self.selected = value;
            return this;
        },
        on: function(event, handler) {
            self.data.on(event, handler);
            return this;
        },
        field: function(field, callback) {
            var config = self.field(field);
            if (callback) {
                callback(config);
                return this;
            }
            return config;
        }
    };
};

QueryResult.prototype.through = function(streamFn) {
    this._through.push(streamFn);
    return this;
};

/**
 * Constructs a new query
 * @param {* =} payload
 * @param {function =} handler
 * @constructor
 */
function Query(payload, handler) {
    events.EventEmitter.call(this);
    this.payload = payload;
    this.handler(handler);
    this.limit = -1;
    this.fields = {};
    this._through = [];
    this._pre = [];
    this._post = [];
}

util.inherits(Query, events.EventEmitter);

/**
 * Default implementation, should always be overridden by specifying a handler
 * @private
 */
Query.prototype._execute = function() {
    throw new Error('Handler method not implemented. Did you forget to call handler()?');
};


/**
 * Sets the query handler
 * @param {function} handler a query handler function
 * @param {Object =} receiver an optional receiver to bind to the handler
 * @return {Query}
 */
Query.prototype.handler = function(handler, receiver) {
    if (handler) {
        this._execute = receiver ? handler.bind(receiver) : handler;
    }

    return this;
};

Query.prototype._isResultCached = function() {
    return P.resolve(this)
        .then(q => q._cacheKey && redis.exists(q._cacheKey));
};

Query.prototype._loadCachedResult = function(reply) {

};

Query.prototype._executeAndCache = function (query, reply) {

};

Query.prototype.execute = function(params) {
    const result = new QueryResult(this)
};

/**
 * Executes a query returning a promise to a query result
 * @param {*=} params runtime query parameters
 * @return {Thenable<QueryResult>} a promise to the query result
 */
Query.prototype.execute = function(params) {
    var self = this;

    // already running
    if (this._result) return this._result;

    this.params = params;

    // already cancelled abort
    if (this.cancelled) return P.reject(new P.CancellationError());

    /**
     * Creates a reply function that may be used once by the handler. If the reply is successful, a shim to the
     * query results is returned for synchronously customizing metadata such as fields, selected, and meta.
     *
     * @return {bluebird|exports|module.exports}
     */
    function execAndReply() {
        return new P(function (resolve, reject) {
            var reply = _.once(function (err, data) {
                if (err && err instanceof Error) return reject(err);

                data = data || err;

                var result = new QueryResult(self, data);

                setImmediate(resolve.bind(null, result));

                return result.shim();
            });

            self._isResultCached()
                .then(cached => cached ? self._loadCachedResult(reply) : self._execute(self, reply))
                .catch(reject);
        });
    }

    function tapChain(interceptors) {
        return function(value) {
            return interceptors.reduce(function (p, interceptor) {
                return p.tap(interceptor);
            }, P.resolve(value));
        };
    }

    this._result = P.resolve(this)
        .tap(tapChain(self._pre))
        .then(execAndReply)
        .tap(tapChain(self._post))
        .cancellable()
        .catch(P.CancellationError, function (e) {
            self._cancel();
            throw e;
        });

    return this._result;
};

/**
 * Updates the query to a cancelled state, emits the cancellation event
 * @private
 */
Query.prototype._cancel = function () {
    this.cancelled = true;
    this.emit('cancel');
};

/**
 * Cancels the query by emitting a 'cancel' event that can be listened to by the handler
 */
Query.prototype.cancel = function() {
    if (this._result && this._result.isPending()) {
        // if already running, then cancel via the promise
        this._result.cancel();
    } else {
        // otherwise set the cancelled bit. execute will short circuit with cancellation error if it is called
        this._cancel();
    }

    return this;
};

/**
 * Synchronously returns a stream that is resolved once the query finishes executing
 * @param params
 * @return {Object} a highland stream
 */
Query.prototype.stream = function(params) {
    var resultPromise = this.execute(params)
        .then(function (result) {
            return result.stream();
        });

    return $(resultPromise).sequence();
};

/**
 * Convenience function for executing a query and getting the results in an array
 * @param {Promise<Object[]>} params
 */
Query.prototype.toArray = function(params) {
    return this.execute(params)
        .then(function (result) {
            return result.toArray();
        });
};

Query.prototype.setLimit = function(limit) {
    // add limit to the query so that the driver may optimize if possible
    this.limit = limit;
    return this;
};

/**
 * Middleware support
 * @param plugin
 * @return {Query}
 */
Query.prototype.use = function(plugin) {
    plugin(this);
    return this;
};

/**
 * Adds a through factory function
 * @param through
 * @return {Query}
 */
Query.prototype.through = function(through) {
    this._through = this._through.concat(through);
    return this;
};

/**
 * Push a pre interceptor
 * @param interceptor
 * @return {Query}
 */
Query.prototype.pre = function(interceptor) {
    this._pre.push(interceptor);
    return this;
};

/**
 * Push a post interceptor
 * @param interceptor
 * @return {Query}
 */
Query.prototype.post = function (interceptor) {
    this._post.push(interceptor);
    return this;
};

/**
 * Emits a progress event
 * @param progress
 */
Query.prototype.progress = function(progress) {
    return this.emit('progress', progress);
};

Query.prototype.cache = function(cacheKey) {
    this._cacheKey = cacheKey;
    return this;
};

QueryResult.prototype.field = Query.prototype.field = function(field, callback) {
    var self = this;

    var fieldConfig = {
        typeMapping: function (type) {
            return this.configure({ typeMapping: type });
        },
        type: function (type) {
            return this.configure({ typeMapping: { type: type } });
        },
        label: function (label) {
            return this.configure({ label: label });
        },
        position: function(position) {
            return this.configure({ position: position });
        },
        configure: function(config){
            self.fields[field] = _.assign({}, self.fields[field], config);
            return this;
        }
    };

    if (callback) {
        callback(fieldConfig);
        return this;
    }

    return fieldConfig;
};

exports.Query = Query;
exports.QueryResult = QueryResult;