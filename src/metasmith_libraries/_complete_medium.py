NAME = "complete"

# Every exchange compound of CarveMe 1.6.6's bacteria universe. CarveMe gapfills on the
# compounds a medium lists, so this opens all of them.
COMPOUNDS = """
    12dgr160 12dgr180 12ppd__R 12ppd__S 13dampp 13ppd 14glucan 15dap 1ag160 1ag180 1ag181d9
    1ag182d9d12 23camp 23ccmp 23cgmp 23cump 23dappa 23dhbzs3 25dkglcn 26dap__M 2ameph 2ddglcn
    2dhglcn 2h3mb 2h3mp 2hxmp 2m35mdntha 2mba 2mbald 2mpa 2obut 2pg 2pglyc 34dhbz 34dhcinm
    34dhpac 34dhphe 35dnta 3amp 3cmp 3gmp 3h4atb 3hcinnm 3hoxpac 3hpppn 3mb 3mba 3oxoadp 3pg
    3ump 4abut 4abutn 4abz 4abzglu 4ahmmp 4crsol 4hba 4hbald 4hbz 4hoxpac 4hoxpacd 4hphac
    4hpro_DC 4hpro_LT 4hptn 4hthr 4oxptn 5aptn 5dglcn 5drib 5g2oxpt 5mcsn 5mdru1p 5mtr 5oxpro
    6apa 6atha 6hnac 6pgc LalaDglu LalaDgluMdap LalaDgluMdapDala LalaLglu Larab Lcyst Ncbmpts
    R3hdec4e R_3h4atba R_3h6atha R_3hcmrs7e R_3hdcaa R_3hdd5ea R_3hdd6e R_3hdda R_3hhdca R_3hhpa
    R_3hhxa R_3hnonaa R_3hocta R_3hpba R_3hpdeca R_3hphpa R_3hphxa R_3hpnona R_3hpocta R_3hppta
    R_3hpt R_3htd58e R_3htd5e R_3httdca T4hcinnm abg4 abt abt__D abt__L ac acac acald acetone
    acgal acgal1p acgam acgam1p acglu acmana acmum acnam acon_C acser actn__R acysbmn ad ade adn
    agm airs akg ala_B ala_L_asp__L ala_L_glu__L ala_L_thr__L ala__D ala__L ala_gln ala_his
    ala_leu alaala alagly alahis alaleu alathr alatrp algac_MG_14 algac_MG_23 algac_MG_32
    algac_MG_41 algac__M all__D alltn amp anhgm arab__D arab__L araban__L arbt arbt6p arg__D
    arg__L argp ascb__L asn__D asn__L aso3 aso4 asp__D asp__L balaala balabala balagly balaleu
    balamd bhb biliverd bmn btd_RR btn btoh but buts butso3 bz bzal bzalc ca2 carn catechol
    cell4 cell6 cellb cgly ch4s chol chols chor chsterol chtbs cinnm cit citr__L cl cmcbtt cmp
    co co2 coa cobalt2 confrl cpgn cpgn_un creat cresol crn crtn csn ctbt cu cu2 cyan cynt
    cys__D cys__L cyst__L cytd d23hb dachi dad_2 dag181d9 dag182d9d12 damp dca dcmp dcyt ddca
    dextrin dgmp dgsn dgudbutn dha dhap dhptd diact dimp din dmanur dmgly dms dmso dmso2 dopa
    drib dtbt dtmp dump duri dxyl dxylnt ecto__L enter etha eths ethso3 etoh f6p fad fald
    fcmcbtt fe2 fe3 fe3dcit fe3dhbzs fe3dhbzs3 fe3hox fe3hox_un fe3mcbtt fe3pyovd_kt feenter
    feoxam feoxam_un fer fmn fol for forglu frmd fru frulys fruur fuc fuc__L fum g1p g3pc g3pe
    g3pg g3pi g3ps g6p g6p_B ga gal gal1p gal_bD galam galct__D galctn__D galctn__L galctr__D
    galman4 galman6 galt galur gam gam6p gcald gdp glc__D glc__aD glcman4 glcman6 glcn glcn__D
    glcr glcur glcur1p gln__L glu__D glu__L glucan4 glucan6 glutar glx gly gly_asn__L gly_asp__L
    gly_cys gly_gln gly_glu__L gly_leu gly_met gly_phe gly_pro__L gly_tyr glyald glyb glyc
    glyc2p glyc3p glyc__R glyclt glycol glygln glyglu glygly glyglygln glymet glyphe glyser gmp
    gsn gthox gthrd gtp gua gudac gudptn guln__L h h2 h2co3 h2o h2o2 h2s ham hco3 hcys__L hdca
    hdcea hexs hgentis hia his__L hisgly hishis hom__L hpta hqn hxa hxan ibt ibtol icit id3acald
    idon__L ile__L imp ind3ac indole inost ins isetac isobuta isocap istfrnA istfrnB istnt ival
    k lac__D lac__L lcts leu__D leu__L leuleu lipoate lmn2 lnlc lys__D lys__L lysglugly lyx__L
    m2but m4po m_xyl madg mal__D mal__L malt malthp malthx maltpt malttr maltttr man man1p man6p
    man6pglyc manb manglyc mantr manttr manur mbdg mcbtt melib meoh met_L_ala__L met__D met__L
    metglcur metox metox__R metsox_R__L metsox_S__L mevR mg2 minohp mmet mn2 mnl mnl1p mobd mso3
    mththf n2 n2o nac ncam nh4 nmn no no2 no3 nona o2 o2s oaa ocdca ocdcea octa octscoa orn
    orn__D orn__L orot oxa p_xyl pac pacald pala pea peamn peng pentso3 pep phe__D phe__L phedca
    phehpa phehxa pheme phenol phenona pheocta phept phpyr pi pime pnto__R ppa ppal ppap ppat
    ppi ppoh pppn ppt prealg_MG_14 prealg_MG_23 prealg_MG_32 prealg_MG_41 prealginate_G pro__D
    pro__L progly prohisglu psclys pser__D pser__L psuri pta ptrc ptsla pydam pydx pydxn
    pyovd_kt pyr quin r5p raffin rbt rib__D ribflv rmn rnam s salc salchs2 salchs2fe salchs4
    salchs4fe salchsx salcn sbt__D sel ser__D ser__L serglugly sheme skm slnt so3 so4 spmd
    srb__L starch stfrnA stfrnB succ sucr sula sulfac tag160 tag180 tag181d9 tag182d9d12 tag__D
    tagur tartr__D tartr__L taur tcb tcynt tet tgt thm thr__L thrp thym thymd tma tmam tmanur
    tmao tnt tol tre tre6p trp__L tsul ttdca ttdcea tton tym tyr__D tyr__L tyrp uaccg uacgam
    udcpdp udcpp udpacgal udpg udpgal udpglcur ump ura urate urea uri vacc val__D val__L vanln
    vanlt xan xmp xtsn xyl3 xyl__D xylan4 xylan8 xylb xylu__L zn2
""".split()

TSV = "exchange\tlower_bound\tupper_bound\tmedium\n" + "".join(
    f"EX_{c}_e\t-10\t1000\t{NAME}\n" for c in COMPOUNDS)
