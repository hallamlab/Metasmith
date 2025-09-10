include { i1 } from './i1'
include { i2 } from './i2'

workflow b1 {
    i1()
    i2()
}
