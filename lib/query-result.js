'use strict';

const _ = require('lodash');
const P = require('bluebird');
const $ = require('highland');
const flow = require('ent-flow');
const PassThrough = require('stream').PassThrough;
const FieldConfigurator = require('./field-configurator').FieldConfigurator;

/**
 * The result of a query. Houses any metadata returned by the driver and provides an interface to begin streaming
 * the result records.
 */
class QueryResult {
    /**
     * Constructs a new QueryResult instance. Usually only called by the invokeWithReply static function
     * @param {Query} query the query object
     * @param {*=} data the result data, which is wrapped into a highland stream
     */
    constructor(query, data) {
        this.query = query;
        this.fields = _.assign({}, query.fields);
        this.throughHandlers = [].concat(query.throughHandlers);
        this._source = this._initDatastream(data).filter(() => !query.cancelled);
        this._passthrough = new PassThrough({ objectMode: true });

        // cancel was called after execute started but before the result was created
        if (query.cancelled) this.cancel();
    }

    /**
     * Initializes the query result's stream
     * @param data
     * @return {*}
     * @private
     */
    _initDatastream(data) {
        data = data || [];
        return this.query.limit >= 0 ? $(data).take(this.query.limit) : $(data);
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
     * @param {function} handlers
     * @example
     * qr.through(s => s.filter(filterFn));
     * @return {QueryResult}
     */
    through(handlers) {
        this.throughHandlers = this.throughHandlers.concat(handlers);
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

            // subscribes to stream events
            on: function(event, handler) {
                self._passthrough.on(event, handler);
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
            .concat(_.constant(this._source.pipe(this._passthrough)))
            .concat(this.throughHandlers);

        // the query result instance will be the context variable passed to all through streams
        return flow.create(this, streams).stream();
    }

    /**
     * Returns a promise that resolves to an array of all data values
     */
    toArray() {
        return new P(resolve => this.stream().toArray(resolve));
    }

    /**
     * Cancels the result by ending the stream
     */
    cancel() {
        this._source.destroy();
        this._passthrough.push(null);
        return this;
    }
}

exports.QueryResult = QueryResult;