Promise = require('bluebird');

import { Model } from 'chaos-orm';
import { Schema } from '../../src';

Model.schema(Schema);

require('./database-spec');
require('./query-spec');
require('./schema-spec');
