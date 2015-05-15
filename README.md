heka-protobuf-filter
=========

Protobuf filter plugin for [Mozilla Heka](http://hekad.readthedocs.org/)

ProtobufFilter
===========


To Build
========

See [Building *hekad* with External Plugins](http://hekad.readthedocs.org/en/latest/installing.html#build-include-externals)
for compiling in plugins.

Edit cmake/plugin_loader.cmake file and add

    add_external_plugin(git https://github.com/michaelgibson/heka-protobuf-filter master)

Build Heka:
	. ./build.sh


Config
======
	[filter_protobuf]
	type = "ProtobufFilter"
	message_matcher = "Fields[decoded] == 'True'"
	flush_interval = 30000
	flush_bytes = 10000
	protobuf_tag = "protobuf_filtered"
	encoder = "ProtobufEncoder"
	delimitter = "\n"
