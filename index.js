/**
 * Module dependencies
 */

var AWS = require('aws-sdk');
var util = require('util');

/**
 * Constants
 */

var SUPPORTED_PLATFORMS = {
    HTTP: 'HTTP'
};

var SUPPORTED_REGIONS = {
    'us-west-2': 'us-west-2'
};

/**
 * Opts verify
 */

function verifyOpts(opts) {
    if (!SUPPORTED_PLATFORMS.hasOwnProperty(opts.platform)) {
        throw new Error(util.format('Unsupported platform "%s".', opts.platform));
    }
    if (!SUPPORTED_REGIONS.hasOwnProperty(opts.region)) {
        throw new Error(util.format('Unsupported region "%s".', opts.region));
    }
    if (typeof(opts.accessKeyId) !== 'string') {
        throw new Error(util.format('Invalid accessKeyId: "%s".', opts.accessKeyId));
    }
    if (typeof(opts.secretAccessKey) !== 'string') {
        throw new Error(util.format('Invalid secretAccessKey: "%s".', opts.secretAccessKey));
    }
}

/**
 * Initialize a new winterfell
 */

function Winterfell(opts) {
    verifyOpts(opts);

    this.platform = opts.platform;

    this.sns = new AWS.SNS({
        region: opts.region,
        apiVersion: opts.apiVersion,
        accessKeyId: opts.accessKeyId,
        secretAccessKey: opts.secretAccessKey
    });
}

/**
 * Returns the region for this instance.
 * @return {String}
 */

Winterfell.prototype.getRegion = function() {
    return this.sns.config.region;
};

/**
 * Returns the specified AWS api version for this instance.
 * @return {String}
 */

Winterfell.prototype.getApiVersion = function() {
    return this.sns.config.apiVersion;
};

/**
 * Publish a message to Amazon SNS
 * @param {Object} options
 * @param {Function} callback
 */

Winterfell.prototype.publishMessage = function(opts, callback) {
    if (!opts.topicArn && !opts.targetArn) {
        throw new Error('Either `topicArn` or `targetArn` needs to be defined.');
    }
    if (!opts.message || typeof(opts.message) !== 'object') {
        throw new Error('Either missing `message` or it\'s not an object.');
    }

    var messageHeaders = {
        Message: JSON.stringify(opts.message),
        MessageStructure: 'json'
    };

    if (opts.topicArn) {
        messageHeaders.TopicArn = opts.topicArn;
    }
    if (opts.targetArn) {
        messageHeaders.TargetArn = opts.targetArn;
    }

    this.sns.publish(messageHeaders, function(err, res) {
        if (err) {
            return callback(err);
        }

        return callback(null, res);
    });
};

/**
 * Retrieves list of topics from Amazon SNS
 * @param {[Object]} options
 * @param {Function} callback
 */

Winterfell.prototype.listTopics = function(nextToken, callback) {
    var cb, token = '';

    if (typeof(nextToken) === 'function') {
        cb = nextToken;
    } else if (typeof(nextToken) === 'string') {
        token = nextToken;
        cb = callback;
    }

    var headers = {
        NextToken: token
    };

    this.sns.listTopics(headers, function(err, res) {
        if (err) {
            return cb(err);
        }

        return cb(null, res);
    });
};

/**
 * Creates a topic that can be subscribed to
 * @param {String} topicName Name to provide for topic
 * @param {Function} callback
 */

Winterfell.prototype.createTopic = function(topicName, callback) {
    var opts = {
        Name: topicName
    };

    this.sns.createTopic(opts, function(err, data) {
        if (err) {
            return callback(err);
        }

        return callback(null, data);
    });
};

/**
 * Deletes a topic and its associated subscriptions
 * @param {String} topicName Name to provide for topic
 * @param {Function} callback
 */

Winterfell.prototype.deleteTopic = function(topicName, callback) {
    var self = this;
    this.getTopic(topicName, function(err, data) {
        var topicArn = data.TopicArn;
        var opts = {
            TopicArn: topicArn
        };

        self.sns.deleteTopic(opts, function(err, data) {
            if (err) {
                return callback(err);
            }

            return callback(null, data);
        });
    });
};

/**
 * Returns a topic along with its required TopicArn.
 * Duplicate of `createTopic` as the SDK will return an existing topic if it
 * has already been created, but it makes the interface cleaner having separate
 * methods.
 */

Winterfell.prototype.getTopic = Winterfell.prototype.createTopic;

/**
 * Create a subscription to an already created topic.
 * (note) currently only tested with HTTP endpoints.
 * @param {Object} opts
 * @param {Function} callback
 */

Winterfell.prototype.createSubscription = function(opts, callback) {
    var params = {
        Protocol: opts.protocol,
        TopicArn: opts.topicArn,
        Endpoint: opts.endpoint
    };

    this.sns.subscribe(params, function(err, data) {
        if (err) {
            return callback(err);
        }

        return callback(null, data);
    });
};

/**
 * Subscribe an endpoint to a topic.
 * This supports both confirming subscriptions and handling the notification
 * itself as Amazon SNS uses uses a POST request to the same URL for both
 * actions.
 * @param {Object} req Request object passed from Express
 * @param {Object} res Response object passed from Express
 * @param {Function} callback Function
 */

Winterfell.prototype.subscribe = function(req, res, callback) {
    var self = this;
    var chunks = [];

    req.on('data', function(chunk) {
        chunks.push(chunk);
    });

    req.on('end', function() {
        var message;

        try {
            message = JSON.parse(chunks.join(''));
        } catch(e) {
            return callback(new Error('Error parsing JSON'));
        }

        if (message.Type === 'SubscriptionConfirmation') {
            return self._confirmSubscription(message, callback);
        } else if (message.Type === 'Notification') {
            return self._handleNotification(message, callback);
        } else {
            return callback(new Error('Invalid request'));
        }
    });

    res.end();
};

/**
 * Private method used to handle the returned data from Amazon wanting to
 * confirm the subscription.
 * @param {Object} data Payload returned from Amazon
 * @param {Function} callback
 */

Winterfell.prototype._confirmSubscription = function(data, callback) {
    var opts = {
        Token: data.Token,
        TopicArn: data.TopicArn
    };

    this.sns.confirmSubscription(opts, function(err, data) {
        if (err) {
            return callback(err);
        }

        callback(null, 'SubscriptionConfirmation', data);
    });
};

/**
 * Private method used to handle the returned data from Amazon wanting to send
 * a noteification.
 * @param {Object} data Payload returned from Amazon
 * @param {Function} callback
 */

Winterfell.prototype._handleNotification = function(data, callback) {
    return callback(null, 'Notification', data);
};

/**
 * Expose `Winterfell`.
 */

Winterfell.SUPPORTED_PLATFORMS = SUPPORTED_PLATFORMS;
Winterfell.SUPPORTED_REGIONS = SUPPORTED_REGIONS;

module.exports = Winterfell;
