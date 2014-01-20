go-sqsReader
============

a golang SQS reader - library and executable. 

This reader is for messages that are composed of new line separated JSON. So
each SQS message can contian one or more pieces of JSON in its message body. 

installation
============

clone this repo into your go filestructure and run go install

(should really make a binary, sorry)

usage
=====

run like

  sqsReader -accessKey=abc123 -accessSecret=xyz456 -endpoint="https://sqs.us-east-1.amazonaws.com/12345678/MyQueue"

and messages will magically appear in standard out.

speed
=====

On an m1.small I can get about 40-50 sqs messages per second out of this. Which
if you wedge 10 JSON blobs into each sqs message this can decode about 400-500
    messages a second.

