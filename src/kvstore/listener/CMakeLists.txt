# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(
    listener_obj OBJECT
    Listener.cpp
    elasticsearch/ESListener.cpp
    kafka/KafkaListener.cpp
    kafka/KafkaAdapter.cpp
)

nebula_add_subdirectory(test)
