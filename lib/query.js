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
    this.execute = _.once(Query.prototype.execute);
    this._through = [];
    this._interceptors = [];
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
 * @return {Promise<QueryResult>} a promise to the query result
 */
Query.prototype.execute = function(params) {
    var self = this;
    this.params = params;

    /**
     * Creates a reply function that may be used once by the handler. If the reply is successful, a shim to the
     * query results is returned for synchronously customizing metadata such as fields, selected, and meta.
     *
     * @param resolve
     * @param reject
     * @return {*}
     */
    function createReply(resolve, reject) {
        return _.once(function(err, data) {
            if (err) return reject(err);

            var result = new QueryResult(self, data);

            setImmediate(resolve.bind(null, result));

            return result.shim();
        });
    }

    //noinspection JSValidateTypes
    var queryPromise = new P(function (resolve, reject) {
        self._execute(self, createReply(resolve, reject));
    });

    return this._interceptors.reduce(function(promise, interceptor) {
        return promise.tap(interceptor);
    }, queryPromise);
};

/**
 * Cancels the query by emitting a 'cancel' event that can be listened to by the handler
 */
Query.prototype.cancel = function() {
    this.emit('cancel');
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

Query.prototype.tap = function(interceptor) {
    this._interceptors.push(interceptor);
    return this;
};

exports.Query = Query;
exports.QueryResult = QueryResult;