package worker

import java.lang.IllegalArgumentException


object Tuple{
    // Tuple(List[Byte]) would be a constructor of tuple.
    def apply(byte: List[Byte]): Tuple = Tuple.fromBytes(byte)

    def fromBytes(byte: List[Byte]): Tuple = {
        if (byte.length != 100)
            throw new IllegalArgumentException("Byte length must be 100")
        else{
            val (key, value) = byte.splitAt(10)
            new Tuple(key, value)
        }
    }

}

class Tuple(key_ :List[Byte], value_ :List[Byte]){
    val key: List[Byte] = key_
    val value: List[Byte] = value_

    def byteListToString(byte_list: List[Byte]): String = {
        (byte_list map {_.toHexString}).mkString(" ")
    }

    override def toString(): String = {
        "TUPLE : " + byteListToString(key) + " |" + byteListToString(value)
    }

    def <(other: Tuple): Boolean = {
        def less(a: List[Byte], b: List[Byte]): Boolean = {
            if (a.isEmpty && b.isEmpty) return false
            else (a.head < b.head) || (a.head == b.head && less(a.tail, b.tail))
        }
        less(key, other.key)
    }

}