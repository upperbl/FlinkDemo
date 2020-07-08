package com.ds.protocol

case class UserBehavior(userId: Long,
                        itemId: Long,
                        categoryId: Int,
                        behavior: String,
                        timestamp: Long)
case class ItemViewCount(itemId: Long,
                         windowEnd: Long,
                         count: Long)