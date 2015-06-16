'use strict';

var events = require('events');
var util = require('util');
var _ = require('lodash');
var flow = require('ent-flow');
var $ = require('highland');
var P = require('bluebird');

/**
 * @param {Query} query
 * @param {Object} data
 * @constructor
 */
function QueryResult(query, data) {
    this.query = query;
    this.cancelled = false;
    this.data = $(data || []);
    this.fields = [];
    this.selected = -1;
    this.query.on('cancel', this.onCancel.bind(this));
}

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

QueryResult.prototype.stream = function() {
    var streams = [].concat(_.identity.bind(null, this.data)).concat(this.query._through);
    return flow.create(this, streams).stream();
};

/**
 * Creates a shim for use in the reply to set properties on the result
 * @return {{fields: Function, meta: Function}}
 */
QueryResult.prototype.shim = function() {
    var self = this;

    return ['fields', 'meta', 'selected'].reduce(function (acc, prop) {
        acc[prop] = function(value) {
            self[prop] = value;
            return this;
        };

        return acc;
    }, {});
};

/**
 * Constructs a new query
 * @param {*} payload
 * @param {function} handler
 * @constructor
 */
function Query(payload, handler) {
    events.EventEmitter.call(this);
    this.payload = payload;
    this.handler(handler);
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

            self._execute(self, reply);
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

exports.Query = Query;
exports.QueryResult = QueryResult;