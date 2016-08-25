'use strict';

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

exports.FieldConfigurator = FieldConfigurator;