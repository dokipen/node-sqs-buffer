/**
 * Sqs write buffer.
 *
 * If your AWS_SECRET_ACCESS_KEY and AWS_ACCESS_KEY_ID, then all you need
 * to pass in is a queueName. region is us-east-1 by default.
 *
 * See examples/simple.js for an example.
 *
 * custom events:
 *
 *   - batchRequest: A batch has been sent but we haven't recieved a response
 *   - batchResponse: Recieved a response for a sent batch.
 *   - bufferEmptied: Useful for waiting until buffer is empty before exiting
 */
var AWS = require('aws-sdk')
  , _ = require('underscore')
  , debug = require('debug')('sqs-buffer')
  , stream = require('stream')
  , inspect = require('util').inspect
  , q = require('q')

function SqsBuffer(awsOpts, streamOpts) {
  if (!(this instanceof SqsBuffer)) {
    return new SqsBuffer(awsOpts, streamOpts)
  }

  stream.Writable.call(this, streamOpts)

  _.extend(this,
           { queueName: null,
             secretAccessKey: null || process.env.AWS_SECRET_ACCESS_KEY,
             accessKeyId: null || process.env.AWS_ACCESS_KEY_ID,
             batchSize: 10,
             sslEnabled: true,
             region: 'us-east-1' },
           awsOpts)

  var awsConfig = {
        accessKeyId: this.accessKeyId,
        secretAccessKey: this.secretAccessKey,
        sslEnabled: this.sslEnabled
      },
      queueUrl = null,
      self = this

  debug(inspect(awsConfig))

  AWS.config.update(awsConfig)

  self.logBuffer = []
  self.inflight = 0  // batches sent but haven't recv response
  self.sqs = new AWS.SQS({region: this.region})

  self.sqs.getQueueUrl({QueueName: this.queueName}, function(err, data) {
    if (err) {
      debug(err.stack)
      self.emit('error', err)
    }
    self.queueUrl = data.QueueUrl
    debug('queue URL: %s', self.queueUrl)
    self.sqsReady = true
    self.emit('ready', self)
  })

  self.on('batchRequest', function(batch, params) {
    self.inflight = self.inflight + 1
    debug('sent batch of %d to %s, params: %s, inflight: %d',
          batch.length, self.queueUrl, inspect(params), self.inflight)
  });

  self.on('batchResponse', function(batch, err, data) {
    self.inflight = self.inflight - 1
    debug('recieved batchResponse: %s, inflight: %d', inspect(data), self.inflight)
    if (self.inflight == 0) {
      self.emit('bufferEmptied')
    }
  });

}

SqsBuffer.prototype = Object.create(stream.Writable.prototype, {
  constructor: { value: SqsBuffer }
})

function _sendBatch(sqs, batch, url, emitter) {


  var entries = batch.map(function(body, id) {
        return { Id: ''+id, MessageBody: JSON.stringify(body) }
      })
    , params = {
        QueueUrl: url,
        Entries: entries
      }

  emitter.emit('batchRequest', batch, params)

  sqs.sendMessageBatch(params, function(err, data) {
    if (err) {
      debug(err.stack)
      emitter.emit('error', err)
    }
    emitter.emit('batchResponse', batch, err, data)
  })
}

SqsBuffer.prototype._send = function() {
  if (this.sqsReady) {
    while (this.logBuffer.length >= this.batchSize) {
      var batch = this.logBuffer.splice(0, this.batchSize)
      _sendBatch(this.sqs, batch, this.queueUrl, this)
    }
  } else {
    debug("Batch ready but sqs isn't")
  }
}

SqsBuffer.prototype._write = function(chunk, _, fn) {
  if (chunk instanceof Buffer) {
    chunk = chunk.toString()
  }
  this.logBuffer.push(chunk)
  if (this.logBuffer.length >= this.batchSize) {
    this._send()
  }
  fn && fn()
}

SqsBuffer.prototype.flush = function() {
  var deferred = q.defer()
  if (this.logBuffer.length == 0) {
    deferred.resolve()
  } else {
    this.on('bufferEmptied', deferred.resolve)
    this._send()
    if (this.logBuffer.length > 0) {
      _sendBatch(this.sqs, this.logBuffer, this.queueUrl, this)
      this.logBuffer = []
    }
  }
  return deferred.promise
}

exports = module.exports = SqsBuffer
