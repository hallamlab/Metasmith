def in(f) {
    return Channel.fromPath(f)
    .splitCsv(header: false)
    .map((row) -> {
        def i = [:] // this will be filld with ${post()}
        return tuple(i, file(row[0]))
    })
}