'use strict';

const _ = require('lodash');
const P = require('bluebird');
const EventEmitter = require('events').EventEmitter;
const flow = require('ent-flow');
const Logger = require('glib').Logger;
const log = new Logger(['query']);
const cachedHandler = require('./redis-cache-handler');

var $ = require('highland');

/**
 * A query handler callback
 * @callback handlerCallback
 * @param {{}} query the query to execute
 * @param {function} reply the reply
 */

/**
 * A utility for configuring field entries in a field hash
 */
class FieldConfigurator {

    /**
     * Constructor
     * @param {{}} fields the fields object keyed by field name
     * @param {string} field the name of the field to configure
     */
    constructor(fields, field) {
        this.fields = fields;
        this.field = field;
    }

    /**
     * Sets the elasticsearch type mapping
     * @param {string} typeMapping the full elasticsearch type mapping
     * @return {FieldConfigurator} the field configurator instance for chaining
     */
    typeMapping(typeMapping) {
        return this.configure({ typeMapping: typeMapping });
    }

    /**
     * Shorthand for simple type mappings
     * @param {string} type the elasticsearch type
     * @example
     * query.field('location').type('geo_point');
     * @return {FieldConfigurator} the field configurator instance for chaining
     */
    type(type) {
        return this.configure({ typeMapping: { type: type } });
    }

    /**
     * Assigns a label to the selected field
     * @param {string} label the field label
     * @example
     * query.field('location').label('Coordinates')
     * @return {FieldConfigurator} the field configurator instance for chaining
     */
    label(label) {
        return this.configure({ label: label });
    }

    /**
     * Sets the default field position
     * @param {number} position the field's position
     * @example
     * query.field('location').position(0)
     * @return {FieldConfigurator} the field configurator instance for chaining
     */
    position(position) {
        return this.configure({ position: position });
    }

    /**
     * Fully configures a field all at once
     * @param {{}} config the field config
     * @example
     * query.field('location').configure({ label:
     * @return {FieldConfigurator} the field configurator instance for chaining
     */
    configure(config) {
        self.fields[this.field] = _.assign({}, self.fields[this.field], config);
        return this;
    }
}

/**
 * The result of a query. Houses any metadata returned by the driver and provides an interface to begin streaming
 * the result records.
 */
class QueryResult {
    /**
     * Constructs a new QueryResult instance. Usually only called by the invokeWithReply static function
     * @param {Query} query the query object
     */
    constructor(query) {
        this.query = query;
        this.fields = _.clone(query.fields);
        this._through = [].concat(query._through);
        this.data([]);
    }

    data(data) {
        this._source = $(data);
        return this;
    }

    /**
     * Configures a field
     * @param {string} field the field name
     * @param {function=} callback an optional callback function for accessing the field configurator
     * @example
     * // with a callback
     * qr
     *  .field('location', fc => fc.type('geo_point').label('Coordinates'))
     *  .field('firstName', fc => fc.label('First Name'));
     *  .field('lastName', fc => fc.label('Last Name'));
     *
     *  // without a callback
     *  qr.field('location').type('geo_point');
     *  qr.field('firstName').label('First Name');
     *  qr.field('lastName').label('Last Name');
     *
     * @return {FieldConfigurator|QueryResult}
     */
    field(field, callback) {
        const fc = new FieldConfigurator(this.fields, field);

        // hit the call back for chaining
        if (callback) {
            callback(fc);
            return this;
        }

        return fc;
    }

    /**
     * Assigns default configuration to fields
     * @param defaults
     * @return {QueryResult}
     */
    fieldDefaults(defaults) {
        _.defaults(this.fields, defaults);
        return this;
    }

    /**
     * Adds a new stream factory to the end of the through stream
     * @param {function} through
     * @example
     * qr.through(s => s.filter(filterFn));
     * @return {QueryResult}
     */
    through(through) {
        this._through = this._through.concat(through);
        return this;
    }

    /**
     * Shims the query result for synchronous configuration by the query handler
     * @return {{fields: fields, selected: selected, field: field}}
     */
    shim() {
        var self = this;

        return {
            // configures multiple fields
            fields: function (value) {
                value = _.isArray(value) ? _.object(value.map(v => [v, {}])) : value;
                self.fields = _.assign({}, value);
                return this;
            },

            // sets the number of selected records (not all records may actually be streamed)
            selected: function (value) {
                self.selected = value;
                return this;
            },

            // configures a specific field
            field: function (field, callback) {
                var config = self.field(field);
                if (callback) {
                    callback(config);
                    return this;
                }
                return config;
            }
        };
    }

    /**
     * Creates a data flow by mapping over the source stream through the through stream mappers
     * @return {Object}
     */
    stream() {
        const streams = []
            .concat(_.constant(this._source))
            .concat(this._through);

        // the query result instance will be the context variable passed to all through streams
        return flow.create(this, streams).stream();
    }

    /**
     * Returns a promise that resolves to an array of all data values
     */
    toArray() {
        return new P(resolve => this.stream().toArray(resolve));
    }
}

const QUERY_DEFAULTS = {
    _limitStream: _.identity,
    _pre: [],
    _through: [],
    _post: [],
    options: {},
    params: {}
};

class Query extends EventEmitter {
    constructor(defn) {
        super();

        _.assign(this, QUERY_DEFAULTS, defn);

        this.execute = _.once(() => this._execute());
    }

    /**
     * Adds a through stream handler
     * @param through
     * @return {Query}
     */
    through(through) {
        this._through = this._through.concat(through);
        return this;
    }

    /**
     * Adds a pre handler
     * @param handler
     * @return {Query}
     */
    pre(handler) {
        this._pre = this._pre.concat(handler);
        return this;
    }

    /**
     * Adds a post handler
     * @param handler
     * @return {Query}
     */
    post(handler) {
        this._post = this._post.concat(handler);
        return this;
    }

    /**
     * Creates a promise chain for invoking interceptors, ignoring any return value
     * @param {[]} interceptors the interceptor functions
     * @return {Function}
     * @private
     */
    _tapChain(interceptors) {
        return function(value) {
            return interceptors.reduce(function (p, interceptor) {
                return p.tap(interceptor);
            }, P.resolve(value));
        };
    }

    /**
     * Invokes the handler function
     * @return {Promise.<QueryResult>}
     * @private
     */
    _invokeHandler() {
        const self = this;

        if (!this._handler) throw new Error('No handler defined. Did you forget to call handler()?');

        return new P(function (resolve) {
            const reply = _.once(function (data) {
                const qr = new QueryResult(self).data(data);

                setImmediate(() => resolve(qr));
                return qr.shim();
            });

            self._handler(self, reply);
        });
    }

    /**
     * Runs the query with the supplied parameters, returning a promise to a QueryResult
     * @return {Promise.<QueryResult>}
     */
    _execute() {
        return P.bind(this)
            .tap(ctx => this.emit('execute'))
            .tap(ctx => this._tapChain(this._pre)(this))
            .then(ctx => this._invokeHandler())
            .tap(r => this._tapChain(this._post)(r))
            .tap(r => this.emit('result', r));
    }

    /**
     * Executes the query, synchronously returning a stream bound to the result's stream
     * @param {{}, []=} params optional query params
     * @return {Stream}
     */
    stream(params) {
        return $.sequence(this.execute(params).then(r => r.stream()));
    }

    /**
     * Executes the query returning a promise that resolves to an array containing all results
     * @param {{}, []=} params optional query params
     * @return {Promise.<{}[]>}
     */
    toArray(params) {
        return this.execute(params).then(r => r.toArray());
    }
}

/**
 * A DSL for building queries and executing them via a "handler" function. This class makes no assumptions about the
 * query definition, or payload, or the format of the result, as long as it can be coerced into an object stream.
 *
 * Query result streams may be intercepted and modified through the use of through stream mapper functions. Each
 * function is passed the current stream and a QueryResult instance.
 *
 * Queries may be executed multiple times, generating a new response and result stream each time.
 *
 * @example
 * <pre>
 * function handler(query, reply) {
 *     // reply() result may be used to provide additional metadata
 *     reply([ { first: 'John', last: 'Doe' }, { first: 'Jane', last: 'Doe' } ])
 *          .field('first', f => f.label('First Name'))
 *          .field('last', f => f.label('Last Name'));
 * }
 *
 * new Query('select * from my_table')
 *      .handler(handler)
 *      .through(s => s.map(r => _.assign(r, { fullName: `${r.first} ${r.last}` })))
 *      .execute()
 *      .then(results => results.stream()) // results have populated field objects
 * </pre>
 */
class QueryBuilder {
    constructor(payload) {
        this.defn = {
            payload: payload,
            options: {},
            _pre: [],
            _post: [],
            _through: []
        };
    }

    /**
     * Sets query's handler function, which is called when the query is executed
     * @param {handlerCallback} handler the handler function
     * @param {{}=} receiver an optional receiver to bind to the handler
     */
    handler(handler, receiver) {
        if (!_.isFunction(handler)) throw new Error('handler must be a function');

        // bind the handler to its receiver, if the handler function is a method
        this.defn._handler = receiver ? handler.bind(receiver) : handler;

        return this;
    }

    /**
     * Sets handler options
     * @param {{}} options
     * @return {QueryBuilder}
     */
    options(options) {
        this.defn.options = _.assign({}, options);
        return this;
    }

    /**
     * Sets a single handler option
     * @param {string} name the option name
     * @param {*} value the option value
     * @example
     * @return {QueryBuilder}
     */
    option(name, value) {
        this.defn.options = _.set(this.defn.options, name, value);
        return this;
    }

    /**
     * Sets the user
     * @param {*} user
     * @return {QueryBuilder}
     */
    user(user) {
        this.defn.user = user;
        return this;
    }

    /**
     * Sets a limit option. The limit is conveyed to the handler for potential query optimizations (e.g. '... LIMIT ').
     * In addition, a limit stream is constructed to halt stream iteration when limit is reached
     *
     * @param limit
     * @return {QueryBuilder}
     */
    limit(limit) {
        this.defn.limit = limit;
        this.defn._limitStream = s => s.take(limit);
        return this;
    }

    /**
     * Configures a field
     * @param {string} field the field name
     * @param {function=} callback an optional callback function for accessing the field configurator
     * @example
     * // with a callback
     * qr
     *  .field('location', fc => fc.type('geo_point').label('Coordinates'))
     *  .field('firstName', fc => fc.label('First Name'));
     *  .field('lastName', fc => fc.label('Last Name'));
     *
     *  // without a callback
     *  qr.field('location').type('geo_point');
     *  qr.field('firstName').label('First Name');
     *  qr.field('lastName').label('Last Name');
     *
     * @return {FieldConfigurator|QueryResult}
     */
    field(field, callback) {
        const fc = new FieldConfigurator(this.defn.fields, field);
        if (callback) {
            callback(fc);
            return this;
        }

        return fc;
    }

    /**
     * Sets the full field configuration
     * @param fields
     * @return {QueryBuilder}
     */
    fields(fields) {
        fields = _.isArray(fields) ? _.object(fields.map(v => [v, {}])) : value;
        this.defn.fields = _.assign({}, fields);
        return this;
    }

    /**
     * Sets the default params on the query config
     * @param params
     * @return {QueryBuilder}
     */
    params(params) {
        this.defn.params = params;
        return this;
    }

    /**
     * Creates a new query instance by merging the current defaults with the specified params
     * @param params
     * @return {Query}
     */
    build(params) {
        return new Query(_.assign(this.defn, { params: params }));
    }

    /**
     * Runs the query with the supplied parameters, returning a promise to a QueryResult
     * @param {{}, []=} params optional query params
     * @return {Promise.<QueryResult>}
     */
    execute(params) {
        return this.build(params).execute();
    }

    /**
     * Executes the query, synchronously returning a stream bound to the result's stream
     * @param {{}, []=} params optional query params
     * @return {Stream}
     */
    stream(params) {
        return this.build(params).stream();
    }

    /**
     * Executes the query returning a promise that resolves to an array containing all results
     * @param {{}, []=} params optional query params
     * @return {Promise.<{}[]>}
     */
    toArray(params) {
        return this.build(params).toArray();
    }

    /**
     * Applies a pre interceptor
     * @param interceptor
     * @return {QueryBuilder}
     */
    pre(interceptor) {
        this.defn._pre = this.defn._pre.concat(interceptor);
        return this;
    }

    /**
     * Applies a post interceptor
     * @param interceptor
     * @return {QueryBuilder}
     */
    post(interceptor) {
        this.defn._post = this.defn._post.concat(interceptor);
        return this;
    }

    /**
     * Appends one or more stream mappers to the through chain
     * @param {function|function[]} streams the stream mappers
     * @return {QueryBuilder}
     */
    through(streams) {
        this.defn._through = this.defn._through.concat(streams);
        return this;
    }
}

exports.Query = QueryBuilder;
exports.QueryResult = QueryResult;

exports.create = payload => new QueryBuilder(payload);

exports.cachedHandler = cachedHandler;