include {t4} from './t4'

workflow {
    main:
    ot4 = t4()
    ot4.g.view()

    publish:
    ot4_g = ot4.g
}

output {
    ot4_g {
        path 'g'
        index { path 'g.csv'}
    }
}