Promise = require('bluebird');

import 'babel-polyfill';
import { Model } from 'chaos-orm';
import { Schema } from '../../src';

Model.definition(Schema);

require('./cursor-spec');
require('./database-spec');
require('./query-spec');
require('./schema-spec');
