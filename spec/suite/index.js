Promise = require('bluebird');

var Model = require('chaos-orm').Model;
var Schema = require('../../src/schema');

Model.definition(Schema);

require('./cursor.spec');
require('./database.spec');
require('./query.spec');
require('./schema.spec');
