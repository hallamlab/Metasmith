#!/usr/bin/env nextflow
nextflow.enable.dsl=2

def projectRootDir = new File(projectDir.toString())
def defaultConfigPath = "${projectDir}/asv_pipeline_nextflow.yml"
def paramsMap = [:]

try {
    paramsMap = workflow.params ? new LinkedHashMap(workflow.params) : [:]
} catch( Throwable ignored ) {
    paramsMap = [:]
}

def inlineKeys = [
    'paths','resources','fastp','merge','filter','unoise',
    'table_filter','filename_patterns','environments','config_root',
    'pipeline_config'
]
def hasInlineConfig = inlineKeys.any { paramsMap.containsKey(it) }

def config
File configFile = null
File configRoot = projectRootDir

def providedConfigPath = null
def providedConfigEnv = System.getenv('ASPIRE_PIPELINE_CONFIG') ?: System.getenv('SPARK_PIPELINE_CONFIG')
if( providedConfigEnv ) {
    providedConfigPath = providedConfigEnv
} else if( paramsMap.containsKey('pipeline_config') ) {
    providedConfigPath = paramsMap.pipeline_config
} else if( paramsMap.containsKey('config') ) {
    providedConfigPath = paramsMap.config
}

if( providedConfigPath ) {
    def configPath = file(providedConfigPath)
    configFile = configPath.toFile()
    if( !configFile.exists() ) {
        exit 1, "Config file not found: ${configFile}"
    }
    config = new groovy.yaml.YamlSlurper().parse(configFile)
    configRoot = configFile.parentFile ?: projectRootDir
    log.info "Loaded config from ${configFile}"
} else if( hasInlineConfig ) {
    config = paramsMap
    if( paramsMap.containsKey('config_root') ) {
        def rootPath = file(paramsMap.config_root)
        configRoot = rootPath.toFile()
    }
    log.info "Using inline Nextflow parameters as configuration."
} else {
    def configPath = file(defaultConfigPath)
    configFile = configPath.toFile()
    if( !configFile.exists() ) {
        exit 1, "Config file not found: ${defaultConfigPath}"
    }
    config = new groovy.yaml.YamlSlurper().parse(configFile)
    configRoot = configFile.parentFile ?: projectRootDir
    log.info "Loaded default config from ${configFile}"
}

def resolvePath = { String pathValue ->
    if( !pathValue ) {
        return null
    }
    def candidate = new File(pathValue)
    if( candidate.isAbsolute() ) {
        return candidate.canonicalPath
    }
    return new File(configRoot, pathValue).canonicalPath
}

def fileMd5(File inputFile) {
    return java.security.MessageDigest.getInstance('MD5').digest(inputFile.bytes).encodeHex().toString()
}

def r1Tokens = normalizeList(config.filename_patterns?.r1_tokens, ['R1','1'])
def r2Tokens = normalizeList(config.filename_patterns?.r2_tokens, ['R2','2'])
assert r1Tokens.size() == r2Tokens.size() : "R1 token count (${r1Tokens.size()}) must match R2 token count (${r2Tokens.size()})"

def extPatterns = compilePatterns(config.filename_patterns?.ext_patterns, ['\\.fastq\\.gz$','\\.fq\\.gz$','\\.fastq$','\\.fq$'])
def stripRegex = config.filename_patterns?.sample_strip_regex ?: '(_S[0-9]+)?(_L[0-9]{3})?(_R[12])?(_[12])?(_001)?$'

def inputDir = resolvePath(config.paths?.input_dir)
def publicOutputDir = resolvePath(config.paths?.output_dir)
assert inputDir : "paths.input_dir must be provided in the YAML config"
assert publicOutputDir : "paths.output_dir must be provided in the YAML config"
def runtimeDir = config.paths?.runtime_dir ? resolvePath(config.paths.runtime_dir) : new File(publicOutputDir, '.aspire').canonicalPath
def outputDir = new File(runtimeDir, 'publication_staging').canonicalPath
def workDirPath = config.paths?.work_dir ? resolvePath(config.paths.work_dir) : new File(runtimeDir, 'nf_work').canonicalPath
def resolvedCondaCacheDir = config.paths?.conda_cache_dir ? resolvePath(config.paths.conda_cache_dir) : new File(runtimeDir, 'conda_cache').canonicalPath
def workDirFile = new File(workDirPath)
workDirFile.mkdirs()
workflow.workDir = java.nio.file.Paths.get(workDirFile.canonicalPath)
log.info "Using Nextflow work directory: ${workflow.workDir}"
log.info "Using publication staging directory: ${outputDir}"
def condaCacheDirFile = new File(resolvedCondaCacheDir)
condaCacheDirFile.mkdirs()
System.setProperty('NXF_CONDA_CACHEDIR', condaCacheDirFile.canonicalPath)
log.info "Using custom Conda cache directory: ${condaCacheDirFile.canonicalPath}"

def allowSingleEnd = (config.resources?.single_end ?: false) as boolean
int hostThreads = Runtime.runtime.availableProcessors()
int sampleThreads = config.resources?.threads ? (config.resources.threads as int) : hostThreads
int pipelineThreads = hostThreads

if( config.fastp && !(config.fastp instanceof Map) ) {
    log.warn "Ignoring non-map fastp configuration (${config.fastp.getClass()?.simpleName})"
}
def fastpConfigMap = (config.fastp instanceof Map) ? config.fastp : [:]
def fastpTrimValues = [
    front_r1: fastpConfigMap.trim_front_r1 != null ? (fastpConfigMap.trim_front_r1 as int) : 0,
    tail_r1 : fastpConfigMap.trim_tail_r1  != null ? (fastpConfigMap.trim_tail_r1  as int) : 0,
    front_r2: fastpConfigMap.trim_front_r2 != null ? (fastpConfigMap.trim_front_r2 as int) : 0,
    tail_r2 : fastpConfigMap.trim_tail_r2  != null ? (fastpConfigMap.trim_tail_r2  as int) : 0
]
if( config.merge && !(config.merge instanceof Map) ) {
    log.warn "Ignoring non-map merge configuration (${config.merge.getClass()?.simpleName})"
}
def mergeConfigMap = (config.merge instanceof Map) ? config.merge : [:]
def mergeMaxDiffs = mergeConfigMap.max_diffs != null ? (mergeConfigMap.max_diffs as int) : 20
def mergeMinOverlap = mergeConfigMap.min_overlap != null ? (mergeConfigMap.min_overlap as int) : 5
def mergeTruncQuality = mergeConfigMap.trunc_quality != null ? (mergeConfigMap.trunc_quality as int) : 5
boolean mergeAllowStagger = (mergeConfigMap.allow_stagger ?: false) as boolean

def dirMap = [
    fastp    : "${outputDir}/fastp",
    merge    : "${outputDir}/merged",
    filter   : "${outputDir}/filtered",
    concat   : "${outputDir}/concat",
    derep    : "${outputDir}/derep",
    sina     : "${outputDir}/sina",
    denoise  : "${outputDir}/denoise",
    nochi    : "${outputDir}/nochimeras",
    asv      : "${outputDir}/ASVs",
    mito     : "${outputDir}/mito",
    taxonomy : "${outputDir}/taxonomy",
    stats    : "${outputDir}/stats",
    logs     : "${outputDir}/logs",
    metadata : "${outputDir}/metadata",
    reference: "${outputDir}/reference"
]
new File(dirMap.concat).mkdirs()
new File(dirMap.sina).mkdirs()
new File(dirMap.asv).mkdirs()
new File(dirMap.mito).mkdirs()
new File(dirMap.taxonomy).mkdirs()
new File(dirMap.stats).mkdirs()
new File(dirMap.logs).mkdirs()
new File(dirMap.metadata).mkdirs()
new File(dirMap.reference).mkdirs()

def sinaReferenceFilename = 'SILVA_138.2_SSURef_NR99_03_07_24_opt.arb'
def defaultSinaReferenceUrl = 'https://www.arb-silva.de/fileadmin/silva_databases/current/Exports/SILVA_138.2_SSURef_NR99_03_07_24_opt.arb.gz'
def defaultTaxonomyTaxUrl = 'https://data.qiime2.org/2024.10/common/silva-138-99-tax.qza'
def defaultTaxonomySeqsUrl = 'https://data.qiime2.org/2024.10/common/silva-138-99-seqs.qza'
def defaultTaxonomyTaxFilename = 'silva-138_2-ssu-nr99-tax.qza'
def defaultTaxonomySeqsFilename = 'silva-138_2-ssu-nr99-seqs-DNA.qza'

def envConfigPath = config.environments?.main
def resolvedEnvPath = envConfigPath ? resolveOptionalPath(envConfigPath, configRoot) : null
def defaultEnvPath = new File("${projectDir}/processes/shared_envs/asv_pipeline.yml").canonicalPath
def condaEnvPath = resolvedEnvPath ?: defaultEnvPath
def condaEnvFile = file(condaEnvPath)
if( !condaEnvFile.exists() ) {
    exit 1, "Conda environment YAML not found: ${condaEnvPath}"
}
log.info "Using Conda/Mamba env definition: ${condaEnvPath}"

def sinaEnvConfigPath = config.environments?.sina
def resolvedSinaEnvPath = sinaEnvConfigPath ? resolveOptionalPath(sinaEnvConfigPath, configRoot) : null
def defaultSinaEnvPath = new File("${projectDir}/processes/shared_envs/sina.yml").canonicalPath
def sinaCondaEnvPath = resolvedSinaEnvPath ?: defaultSinaEnvPath
def sinaEnvFile = file(sinaCondaEnvPath)
if( !sinaEnvFile.exists() ) {
    exit 1, "SINA conda environment YAML not found: ${sinaCondaEnvPath}"
}
log.info "Using SINA Conda/Mamba env definition: ${sinaCondaEnvPath}"

def taxonomyEnvConfigPath = config.environments?.taxonomy
def resolvedTaxonomyEnvPath = taxonomyEnvConfigPath ? resolveOptionalPath(taxonomyEnvConfigPath, configRoot) : null
def defaultTaxonomyEnvPath = new File("${projectDir}/processes/shared_envs/qiime2.yml").canonicalPath
def taxonomyCondaEnvPath = resolvedTaxonomyEnvPath ?: defaultTaxonomyEnvPath
def taxonomyEnvFile = file(taxonomyCondaEnvPath)
if( !taxonomyEnvFile.exists() ) {
    exit 1, "Taxonomy conda environment YAML not found: ${taxonomyCondaEnvPath}"
}
log.info "Using taxonomy Conda/Mamba env definition: ${taxonomyCondaEnvPath}"

def mitomasterEnvConfigPath = config.environments?.mitomaster
def resolvedMitomasterEnvPath = mitomasterEnvConfigPath ? resolveOptionalPath(mitomasterEnvConfigPath, configRoot) : null
def defaultMitomasterEnvPath = new File("${projectDir}/processes/shared_envs/mitomaster.yml").canonicalPath
def mitomasterCondaEnvPath = resolvedMitomasterEnvPath ?: defaultMitomasterEnvPath
def mitomasterEnvFile = file(mitomasterCondaEnvPath)
if( !mitomasterEnvFile.exists() ) {
    exit 1, "MITOMASTER conda environment YAML not found: ${mitomasterCondaEnvPath}"
}
log.info "Using MITOMASTER Conda/Mamba env definition: ${mitomasterCondaEnvPath}"

def mitoCheckerEnvConfigPath = config.environments?.mito_checker
def resolvedMitoCheckerEnvPath = mitoCheckerEnvConfigPath ? resolveOptionalPath(mitoCheckerEnvConfigPath, configRoot) : null
def defaultMitoCheckerEnvPath = new File("${projectDir}/processes/shared_envs/mito_checker.yml").canonicalPath
def mitoCheckerCondaEnvPath = resolvedMitoCheckerEnvPath ?: defaultMitoCheckerEnvPath
def mitoCheckerEnvFile = file(mitoCheckerCondaEnvPath)
if( !mitoCheckerEnvFile.exists() ) {
    exit 1, "Mito checker conda environment YAML not found: ${mitoCheckerCondaEnvPath}"
}
log.info "Using mito checker Conda/Mamba env definition: ${mitoCheckerCondaEnvPath}"

def filterCountsEnvConfigPath = config.environments?.filter_counts
def resolvedFilterCountsEnvPath = filterCountsEnvConfigPath ? resolveOptionalPath(filterCountsEnvConfigPath, configRoot) : null
def defaultFilterCountsEnvPath = new File("${projectDir}/processes/shared_envs/filter_counts.yml").canonicalPath
def filterCountsCondaEnvPath = resolvedFilterCountsEnvPath ?: defaultFilterCountsEnvPath
def filterCountsEnvFile = file(filterCountsCondaEnvPath)
if( !filterCountsEnvFile.exists() ) {
    exit 1, "filter_counts conda environment YAML not found: ${filterCountsCondaEnvPath}"
}
log.info "Using filter_counts Conda/Mamba env definition: ${filterCountsCondaEnvPath}"

def generalStatsEnvConfigPath = config.environments?.general_stats
def resolvedGeneralStatsEnvPath = generalStatsEnvConfigPath ? resolveOptionalPath(generalStatsEnvConfigPath, configRoot) : null
def defaultGeneralStatsEnvPath = new File("${projectDir}/processes/shared_envs/general_stats.yml").canonicalPath
def generalStatsCondaEnvPath = resolvedGeneralStatsEnvPath ?: defaultGeneralStatsEnvPath
def generalStatsEnvFile = file(generalStatsCondaEnvPath)
if( !generalStatsEnvFile.exists() ) {
    exit 1, "general_stats conda environment YAML not found: ${generalStatsCondaEnvPath}"
}
log.info "Using general_stats Conda/Mamba env definition: ${generalStatsCondaEnvPath}"

def sankeyEnvConfigPath = config.environments?.sankey
def resolvedSankeyEnvPath = sankeyEnvConfigPath ? resolveOptionalPath(sankeyEnvConfigPath, configRoot) : null
def defaultSankeyEnvPath = new File("${projectDir}/processes/shared_envs/sankey.yml").canonicalPath
def sankeyCondaEnvPath = resolvedSankeyEnvPath ?: defaultSankeyEnvPath
def sankeyEnvFile = file(sankeyCondaEnvPath)
if( !sankeyEnvFile.exists() ) {
    exit 1, "Sankey conda environment YAML not found: ${sankeyCondaEnvPath}"
}
log.info "Using Sankey Conda/Mamba env definition: ${sankeyCondaEnvPath}"

def plotMetadataEnvConfigPath = config.environments?.plot_metadata
def resolvedPlotMetadataEnvPath = plotMetadataEnvConfigPath ? resolveOptionalPath(plotMetadataEnvConfigPath, configRoot) : null
def defaultPlotMetadataEnvPath = new File("${projectDir}/processes/shared_envs/plot_metadata.yml").canonicalPath
def plotMetadataCondaEnvPath = resolvedPlotMetadataEnvPath ?: defaultPlotMetadataEnvPath
def plotMetadataEnvFile = file(plotMetadataCondaEnvPath)
if( !plotMetadataEnvFile.exists() ) {
    exit 1, "Plot metadata conda environment YAML not found: ${plotMetadataCondaEnvPath}"
}
log.info "Using plot metadata Conda/Mamba env definition: ${plotMetadataCondaEnvPath}"

def batchCorrectionEnvConfigPath = config.environments?.batch_correction
def resolvedBatchCorrectionEnvPath = batchCorrectionEnvConfigPath ? resolveOptionalPath(batchCorrectionEnvConfigPath, configRoot) : null
def defaultBatchCorrectionEnvPath = new File("${projectDir}/processes/shared_envs/asv_batch_correction.yml").canonicalPath
def batchCorrectionCondaEnvPath = resolvedBatchCorrectionEnvPath ?: defaultBatchCorrectionEnvPath
def batchCorrectionEnvFile = file(batchCorrectionCondaEnvPath)
if( !batchCorrectionEnvFile.exists() ) {
    exit 1, "Batch correction conda environment YAML not found: ${batchCorrectionCondaEnvPath}"
}
log.info "Using batch correction Conda/Mamba env definition: ${batchCorrectionCondaEnvPath}"

def outlierEnvConfigPath = config.environments?.outlier_checker
def resolvedOutlierEnvPath = outlierEnvConfigPath ? resolveOptionalPath(outlierEnvConfigPath, configRoot) : null
def defaultOutlierEnvPath = new File("${projectDir}/processes/shared_envs/outlier_checker.yml").canonicalPath
def outlierCondaEnvPath = resolvedOutlierEnvPath ?: defaultOutlierEnvPath
def outlierEnvFile = file(outlierCondaEnvPath)
if( !outlierEnvFile.exists() ) {
    exit 1, "Outlier checker conda environment YAML not found: ${outlierCondaEnvPath}"
}
log.info "Using outlier checker Conda/Mamba env definition: ${outlierCondaEnvPath}"

def collectorsEnvConfigPath = config.environments?.collectors_curve
def resolvedCollectorsEnvPath = collectorsEnvConfigPath ? resolveOptionalPath(collectorsEnvConfigPath, configRoot) : null
def defaultCollectorsEnvPath = new File("${projectDir}/processes/shared_envs/collectors_curve.yml").canonicalPath
def collectorsCondaEnvPath = resolvedCollectorsEnvPath ?: defaultCollectorsEnvPath
def collectorsEnvFile = file(collectorsCondaEnvPath)
if( !collectorsEnvFile.exists() ) {
    exit 1, "Collectors curve conda environment YAML not found: ${collectorsCondaEnvPath}"
}
log.info "Using collectors curve Conda/Mamba env definition: ${collectorsCondaEnvPath}"

def diversityEnvConfigPath = config.environments?.diversity
def resolvedDiversityEnvPath = diversityEnvConfigPath ? resolveOptionalPath(diversityEnvConfigPath, configRoot) : null
def diversityCondaEnvPath = resolvedDiversityEnvPath ?: new File("${projectDir}/processes/diversity_analysis/env.yml").canonicalPath
def diversityEnvFile = file(diversityCondaEnvPath)
if( !diversityEnvFile.exists() ) {
    exit 1, "Diversity conda environment YAML not found: ${diversityCondaEnvPath}"
}
log.info "Using diversity Conda/Mamba env definition: ${diversityCondaEnvPath}"

def indicspeciesEnvConfigPath = config.environments?.indicspecies
def resolvedIndicspeciesEnvPath = indicspeciesEnvConfigPath ? resolveOptionalPath(indicspeciesEnvConfigPath, configRoot) : null
def indicspeciesCondaEnvPath = resolvedIndicspeciesEnvPath ?: new File("${projectDir}/processes/indicspecies/env.yml").canonicalPath
def indicspeciesEnvFile = file(indicspeciesCondaEnvPath)
if( !indicspeciesEnvFile.exists() ) {
    exit 1, "Indicspecies conda environment YAML not found: ${indicspeciesCondaEnvPath}"
}
log.info "Using indicspecies Conda/Mamba env definition: ${indicspeciesCondaEnvPath}"

def clustermapsEnvConfigPath = config.environments?.clustermaps
def resolvedClustermapsEnvPath = clustermapsEnvConfigPath ? resolveOptionalPath(clustermapsEnvConfigPath, configRoot) : null
def clustermapsCondaEnvPath = resolvedClustermapsEnvPath ?: new File("${projectDir}/processes/clustermaps/env.yml").canonicalPath
def clustermapsEnvFile = file(clustermapsCondaEnvPath)
if( !clustermapsEnvFile.exists() ) {
    exit 1, "Clustermaps conda environment YAML not found: ${clustermapsCondaEnvPath}"
}
log.info "Using clustermaps Conda/Mamba env definition: ${clustermapsCondaEnvPath}"

def spieceasiEnvConfigPath = config.environments?.spieceasi
def resolvedSpieceasiEnvPath = spieceasiEnvConfigPath ? resolveOptionalPath(spieceasiEnvConfigPath, configRoot) : null
def spieceasiCondaEnvPath = resolvedSpieceasiEnvPath ?: new File("${projectDir}/processes/spieceasi/env.yml").canonicalPath
def spieceasiEnvFile = file(spieceasiCondaEnvPath)
if( !spieceasiEnvFile.exists() ) {
    exit 1, "SPIEC-EASI conda environment YAML not found: ${spieceasiCondaEnvPath}"
}
log.info "Using SPIEC-EASI Conda/Mamba env definition: ${spieceasiCondaEnvPath}"

def networkEnvConfigPath = config.environments?.network
def resolvedNetworkEnvPath = networkEnvConfigPath ? resolveOptionalPath(networkEnvConfigPath, configRoot) : null
def networkCondaEnvPath = resolvedNetworkEnvPath ?: new File("${projectDir}/processes/graph_network/env.yml").canonicalPath
def networkEnvFile = file(networkCondaEnvPath)
if( !networkEnvFile.exists() ) {
    exit 1, "Network conda environment YAML not found: ${networkCondaEnvPath}"
}
log.info "Using network Conda/Mamba env definition: ${networkCondaEnvPath}"

def networkModulesEnvConfigPath = config.environments?.network_modules
def resolvedNetworkModulesEnvPath = networkModulesEnvConfigPath ? resolveOptionalPath(networkModulesEnvConfigPath, configRoot) : null
def networkModulesCondaEnvPath = resolvedNetworkModulesEnvPath ?: spieceasiCondaEnvPath
def networkModulesEnvFile = file(networkModulesCondaEnvPath)
if( !networkModulesEnvFile.exists() ) {
    exit 1, "Network modules conda environment YAML not found: ${networkModulesCondaEnvPath}"
}
log.info "Using network modules Conda/Mamba env definition: ${networkModulesCondaEnvPath}"

def masterSummaryEnvConfigPath = config.environments?.master_summary
def resolvedMasterSummaryEnvPath = masterSummaryEnvConfigPath ? resolveOptionalPath(masterSummaryEnvConfigPath, configRoot) : null
def masterSummaryCondaEnvPath = resolvedMasterSummaryEnvPath ?: new File("${projectDir}/processes/master_summary/env.yml").canonicalPath
def masterSummaryEnvFile = file(masterSummaryCondaEnvPath)
if( !masterSummaryEnvFile.exists() ) {
    exit 1, "Master summary conda environment YAML not found: ${masterSummaryCondaEnvPath}"
}
log.info "Using master summary Conda/Mamba env definition: ${masterSummaryCondaEnvPath}"

def asvMagLinkEnvConfigPath = config.environments?.asv_mag_link
def resolvedAsvMagLinkEnvPath = asvMagLinkEnvConfigPath ? resolveOptionalPath(asvMagLinkEnvConfigPath, configRoot) : null
def asvMagLinkCondaEnvPath = resolvedAsvMagLinkEnvPath ?: new File("${projectDir}/processes/asv_mag_link/env.yml").canonicalPath
def asvMagLinkEnvFile = file(asvMagLinkCondaEnvPath)
if( !asvMagLinkEnvFile.exists() ) {
    exit 1, "ASV-MAG linking conda environment YAML not found: ${asvMagLinkCondaEnvPath}"
}
log.info "Using ASV-MAG linking Conda/Mamba env definition: ${asvMagLinkCondaEnvPath}"

def asvMagNetworkEnvConfigPath = config.environments?.asv_mag_network
def resolvedAsvMagNetworkEnvPath = asvMagNetworkEnvConfigPath ? resolveOptionalPath(asvMagNetworkEnvConfigPath, configRoot) : null
def asvMagNetworkCondaEnvPath = resolvedAsvMagNetworkEnvPath ?: new File("${projectDir}/processes/asv_mag_network/env.yml").canonicalPath
def asvMagNetworkEnvFile = file(asvMagNetworkCondaEnvPath)
if( !asvMagNetworkEnvFile.exists() ) {
    exit 1, "ASV-MAG network conda environment YAML not found: ${asvMagNetworkCondaEnvPath}"
}
log.info "Using ASV-MAG network Conda/Mamba env definition: ${asvMagNetworkCondaEnvPath}"

def powerAnalysisEnvConfigPath = config.environments?.group_power_analysis ?: config.environments?.power_analysis
def resolvedPowerAnalysisEnvPath = powerAnalysisEnvConfigPath ? resolveOptionalPath(powerAnalysisEnvConfigPath, configRoot) : null
def powerAnalysisCondaEnvPath = resolvedPowerAnalysisEnvPath ?: new File("${projectDir}/processes/power_analysis_pipeline/env.yml").canonicalPath
def powerAnalysisEnvFile = file(powerAnalysisCondaEnvPath)
if( !powerAnalysisEnvFile.exists() ) {
    exit 1, "Power analysis conda environment YAML not found: ${powerAnalysisCondaEnvPath}"
}
log.info "Using power analysis Conda/Mamba env definition: ${powerAnalysisCondaEnvPath}"

def taxonomyPatientAwareEnvConfigPath = config.environments?.taxonomy_group_association ?: config.environments?.taxonomy_patient_aware
def resolvedTaxonomyPatientAwareEnvPath = taxonomyPatientAwareEnvConfigPath ? resolveOptionalPath(taxonomyPatientAwareEnvConfigPath, configRoot) : null
def taxonomyPatientAwareCondaEnvPath = resolvedTaxonomyPatientAwareEnvPath ?: new File("${projectDir}/processes/taxonomy_patient_aware/env.yml").canonicalPath
def taxonomyPatientAwareEnvFile = file(taxonomyPatientAwareCondaEnvPath)
if( !taxonomyPatientAwareEnvFile.exists() ) {
    exit 1, "Taxonomy patient-aware conda environment YAML not found: ${taxonomyPatientAwareCondaEnvPath}"
}
log.info "Using taxonomy patient-aware Conda/Mamba env definition: ${taxonomyPatientAwareCondaEnvPath}"

def lungStatusAnalysisEnvConfigPath = config.environments?.paired_group_contrast ?: config.environments?.lung_status_analysis
def resolvedLungStatusAnalysisEnvPath = lungStatusAnalysisEnvConfigPath ? resolveOptionalPath(lungStatusAnalysisEnvConfigPath, configRoot) : null
def lungStatusAnalysisCondaEnvPath = resolvedLungStatusAnalysisEnvPath ?: new File("${projectDir}/processes/lung_status_analysis/env.yml").canonicalPath
def lungStatusAnalysisEnvFile = file(lungStatusAnalysisCondaEnvPath)
if( !lungStatusAnalysisEnvFile.exists() ) {
    exit 1, "Lung status analysis conda environment YAML not found: ${lungStatusAnalysisCondaEnvPath}"
}
log.info "Using lung status analysis Conda/Mamba env definition: ${lungStatusAnalysisCondaEnvPath}"

def plotUpsetEnvConfigPath = config.environments?.plot_upset
def resolvedPlotUpsetEnvPath = plotUpsetEnvConfigPath ? resolveOptionalPath(plotUpsetEnvConfigPath, configRoot) : null
def plotUpsetCondaEnvPath = resolvedPlotUpsetEnvPath ?: new File("${projectDir}/processes/plot_upset/env.yml").canonicalPath
def plotUpsetEnvFile = file(plotUpsetCondaEnvPath)
if( !plotUpsetEnvFile.exists() ) {
    exit 1, "Plot Upset conda environment YAML not found: ${plotUpsetCondaEnvPath}"
}
log.info "Using Plot Upset Conda/Mamba env definition: ${plotUpsetCondaEnvPath}"

def bubbleplotterEnvConfigPath = config.environments?.bubbleplotter
def resolvedBubbleplotterEnvPath = bubbleplotterEnvConfigPath ? resolveOptionalPath(bubbleplotterEnvConfigPath, configRoot) : null
def bubbleplotterCondaEnvPath = resolvedBubbleplotterEnvPath ?: new File("${projectDir}/processes/bubbleplotter/env.yml").canonicalPath
def bubbleplotterEnvFile = file(bubbleplotterCondaEnvPath)
if( !bubbleplotterEnvFile.exists() ) {
    exit 1, "Bubbleplotter conda environment YAML not found: ${bubbleplotterCondaEnvPath}"
}
log.info "Using bubbleplotter Conda/Mamba env definition: ${bubbleplotterCondaEnvPath}"

def umapClusteringEnvConfigPath = config.environments?.umap_clustering
def resolvedUmapClusteringEnvPath = umapClusteringEnvConfigPath ? resolveOptionalPath(umapClusteringEnvConfigPath, configRoot) : null
def umapClusteringCondaEnvPath = resolvedUmapClusteringEnvPath ?: new File("${projectDir}/processes/umap_clustering/env.yml").canonicalPath
def umapClusteringEnvFile = file(umapClusteringCondaEnvPath)
if( !umapClusteringEnvFile.exists() ) {
    exit 1, "UMAP clustering conda environment YAML not found: ${umapClusteringCondaEnvPath}"
}
log.info "Using UMAP clustering Conda/Mamba env definition: ${umapClusteringCondaEnvPath}"

def vocCorrelationEnvConfigPath = config.environments?.voc_correlation
def resolvedVocCorrelationEnvPath = vocCorrelationEnvConfigPath ? resolveOptionalPath(vocCorrelationEnvConfigPath, configRoot) : null
def defaultVocCorrelationEnvPath = new File("${projectDir}/processes/voc_correlation/env.yml").canonicalPath
def vocCorrelationCondaEnvPath = resolvedVocCorrelationEnvPath ?: defaultVocCorrelationEnvPath
def vocCorrelationEnvFile = file(vocCorrelationCondaEnvPath)
if( !vocCorrelationEnvFile.exists() ) {
    exit 1, "VOC correlation conda environment YAML not found: ${vocCorrelationCondaEnvPath}"
}
log.info "Using VOC correlation Conda/Mamba env definition: ${vocCorrelationCondaEnvPath}"

def measurementAssociationEnvConfigPath = config.environments?.measurement_association
def resolvedMeasurementAssociationEnvPath = measurementAssociationEnvConfigPath ? resolveOptionalPath(measurementAssociationEnvConfigPath, configRoot) : null
def defaultMeasurementAssociationEnvPath = new File("${projectDir}/processes/measurement_association/env.yml").canonicalPath
def measurementAssociationCondaEnvPath = resolvedMeasurementAssociationEnvPath ?: defaultMeasurementAssociationEnvPath
def measurementAssociationEnvFile = file(measurementAssociationCondaEnvPath)
if( !measurementAssociationEnvFile.exists() ) {
    exit 1, "Measurement association conda environment YAML not found: ${measurementAssociationCondaEnvPath}"
}
log.info "Using measurement association Conda/Mamba env definition: ${measurementAssociationCondaEnvPath}"

def groupingDiagnosticsEnvConfigPath = config.environments?.grouping_diagnostics
def resolvedGroupingDiagnosticsEnvPath = groupingDiagnosticsEnvConfigPath ? resolveOptionalPath(groupingDiagnosticsEnvConfigPath, configRoot) : null
def defaultGroupingDiagnosticsEnvPath = new File("${projectDir}/processes/grouping_diagnostics/env.yml").canonicalPath
def groupingDiagnosticsCondaEnvPath = resolvedGroupingDiagnosticsEnvPath ?: defaultGroupingDiagnosticsEnvPath
def groupingDiagnosticsEnvFile = file(groupingDiagnosticsCondaEnvPath)
if( !groupingDiagnosticsEnvFile.exists() ) {
    exit 1, "Grouping diagnostics conda environment YAML not found: ${groupingDiagnosticsCondaEnvPath}"
}
log.info "Using grouping diagnostics Conda/Mamba env definition: ${groupingDiagnosticsCondaEnvPath}"

def groupLabelAugmentationEnvConfigPath = config.environments?.group_label_augmentation
def resolvedGroupLabelAugmentationEnvPath = groupLabelAugmentationEnvConfigPath ? resolveOptionalPath(groupLabelAugmentationEnvConfigPath, configRoot) : null
def defaultGroupLabelAugmentationEnvPath = new File("${projectDir}/processes/group_label_augmentation/env.yml").canonicalPath
def groupLabelAugmentationCondaEnvPath = resolvedGroupLabelAugmentationEnvPath ?: defaultGroupLabelAugmentationEnvPath
def groupLabelAugmentationEnvFile = file(groupLabelAugmentationCondaEnvPath)
if( !groupLabelAugmentationEnvFile.exists() ) {
    exit 1, "Group-label augmentation conda environment YAML not found: ${groupLabelAugmentationCondaEnvPath}"
}
log.info "Using group-label augmentation Conda/Mamba env definition: ${groupLabelAugmentationCondaEnvPath}"

def configuredManifestPath = config.paths?.manifest ? resolveOptionalPath(config.paths.manifest, configRoot) : null
def sampleRecords
if( configuredManifestPath ) {
    sampleRecords = loadManifestSamples(configuredManifestPath)
} else {
    sampleRecords = collectSampleRecords(inputDir, r1Tokens, r2Tokens, extPatterns, stripRegex, allowSingleEnd)
}
if( !sampleRecords ) {
    exit 1, configuredManifestPath ? "No usable entries detected in manifest ${configuredManifestPath}" : "No usable FASTQ files detected in ${inputDir}"
}
def manifestPath = writeNormalizedManifest(
    sampleRecords,
    new File(dirMap.metadata, 'run_manifest.tsv'),
    configuredManifestPath
)
log.info "Discovered ${sampleRecords.size()} input items from ${configuredManifestPath ? "manifest ${configuredManifestPath}" : inputDir}. Normalized manifest: ${manifestPath}. Sample threads=${sampleThreads}, default threads=${pipelineThreads}"

Channel
    .from(sampleRecords)
    .map { rec ->
        def meta = [ sample_id: rec.sample_id, paired: rec.paired ]
        def r1File = file(rec.r1)
        def r2File = (rec.paired && rec.r2) ? file(rec.r2) : r1File
        tuple(meta, r1File, r2File)
    }
    .set { raw_reads }

def tabFilterScript = resolveOptionalPath(config.table_filter?.script, configRoot) ?: "${projectDir}/processes/filter_table/filter_ASV_table.py"
def tableScriptFile = file(tabFilterScript)
if( !tableScriptFile.exists() ) {
    exit 1, "Table filter script not found: ${tabFilterScript}"
}

def concatConfig = config.concat ?: [:]
def concatRelabelEnabled = concatConfig.containsKey('relabel') ? (concatConfig.relabel as boolean) : true
def concatLabelSep = concatConfig.label_sep ?: ':'
def filterConfig = config.filter ?: [:]
def filterMaxEe = filterConfig.max_ee != null ? (filterConfig.max_ee as double) : 1.0d
def filterMinLen = filterConfig.min_len != null ? (filterConfig.min_len as int) : 245
def filterMaxLen = filterConfig.max_len != null ? (filterConfig.max_len as int) : 1500

def parseSinaScriptFile = new File("${projectDir}/processes/sina_trim/parse_sina_log.py")
if( !parseSinaScriptFile.exists() ) {
    exit 1, "parse_sina_log.py not found in project directory"
}
def parseSinaScriptPath = parseSinaScriptFile.canonicalPath
def trimSinaScriptFile = new File("${projectDir}/processes/sina_trim/trim_v_sina.py")
if( !trimSinaScriptFile.exists() ) {
    exit 1, "trim_v_sina.py not found in project directory"
}
def trimSinaScriptPath = trimSinaScriptFile.canonicalPath
def mitomasterScriptFile = new File("${projectDir}/processes/mitomaster/mitomaster.py")
if( !mitomasterScriptFile.exists() ) {
    exit 1, "mitomaster.py not found in project directory"
}
def mitomasterScriptPath = mitomasterScriptFile.canonicalPath
def mitoCheckerScriptFile = new File("${projectDir}/processes/mito_decontam/mito_checker.py")
if( !mitoCheckerScriptFile.exists() ) {
    exit 1, "mito_checker.py not found in project directory"
}
def mitoCheckerScriptPath = mitoCheckerScriptFile.canonicalPath
def sankeyScriptFile = new File("${projectDir}/processes/sankey/sankey_builder.py")
if( !sankeyScriptFile.exists() ) {
    exit 1, "sankey_builder.py not found in project directory"
}
def sankeyScriptPath = sankeyScriptFile.canonicalPath
def sankeyScriptHash = fileMd5(sankeyScriptFile)
def plotMetadataScriptFile = new File("${projectDir}/processes/plot_metadata/plot_metadata.py")
if( !plotMetadataScriptFile.exists() ) {
    exit 1, "plot_metadata.py not found in project directory"
}
def plotMetadataScriptPath = plotMetadataScriptFile.canonicalPath
def plotMetadataScriptHash = fileMd5(plotMetadataScriptFile)
def batchCorrectionScriptFile = new File("${projectDir}/processes/asv_batch_correction/asv_batch_correction.py")
if( !batchCorrectionScriptFile.exists() ) {
    exit 1, "asv_batch_correction.py not found in project directory"
}
def batchCorrectionScriptPath = batchCorrectionScriptFile.canonicalPath
def outlierCheckerScriptFile = new File("${projectDir}/processes/outlier_checker/outlier_checker.py")
if( !outlierCheckerScriptFile.exists() ) {
    exit 1, "outlier_checker.py not found in project directory"
}
def outlierCheckerScriptPath = outlierCheckerScriptFile.canonicalPath
def collectorsCurveScriptFile = new File("${projectDir}/processes/collectors_curve/collectors_curve.py")
if( !collectorsCurveScriptFile.exists() ) {
    exit 1, "collectors_curve.py not found in project directory"
}
def collectorsCurveScriptPath = collectorsCurveScriptFile.canonicalPath
def filterCountsScriptFile = new File("${projectDir}/processes/filter_counts/filter_nontarget.py")
if( !filterCountsScriptFile.exists() ) {
    exit 1, "filter_nontarget.py not found in project directory"
}
def filterCountsScriptPath = filterCountsScriptFile.canonicalPath
def filterCountsScriptHash = fileMd5(filterCountsScriptFile)
def calcDivScriptFile = new File("${projectDir}/processes/diversity_analysis/calc_div.py")
if( !calcDivScriptFile.exists() ) {
    exit 1, "calc_div.py not found in project directory"
}
def calcDivScriptPath = calcDivScriptFile.canonicalPath
def plotDiversityScriptFile = new File("${projectDir}/processes/diversity_analysis/plot_diversity.py")
if( !plotDiversityScriptFile.exists() ) {
    exit 1, "plot_diversity.py not found in project directory"
}
def plotDiversityScriptPath = plotDiversityScriptFile.canonicalPath
def plotUpsetScriptFile = new File("${projectDir}/processes/plot_upset/plot_upset.py")
if( !plotUpsetScriptFile.exists() ) {
    exit 1, "plot_upset.py not found in project directory"
}
def plotUpsetScriptPath = plotUpsetScriptFile.canonicalPath
def bubbleplotterScriptFile = new File("${projectDir}/processes/bubbleplotter/bubbleplotter.py")
if( !bubbleplotterScriptFile.exists() ) {
    exit 1, "bubbleplotter.py not found in project directory"
}
def bubbleplotterScriptPath = bubbleplotterScriptFile.canonicalPath
def umapClusteringScriptFile = new File("${projectDir}/processes/umap_clustering/umap_clustering.py")
if( !umapClusteringScriptFile.exists() ) {
    exit 1, "umap_clustering.py not found in project directory"
}
def umapClusteringScriptPath = umapClusteringScriptFile.canonicalPath
def indicspeciesScriptFile = new File("${projectDir}/processes/indicspecies/run_indicspecies.R")
if( !indicspeciesScriptFile.exists() ) {
    exit 1, "run_indicspecies.R not found in project directory"
}
def indicspeciesScriptPath = indicspeciesScriptFile.canonicalPath
def plotIndicspeciesScriptFile = new File("${projectDir}/processes/indicspecies_plots/plot_indicspecies.py")
if( !plotIndicspeciesScriptFile.exists() ) {
    exit 1, "plot_indicspecies.py not found in project directory"
}
def plotIndicspeciesScriptPath = plotIndicspeciesScriptFile.canonicalPath
def plotIndicspeciesAlignedScriptFile = new File("${projectDir}/processes/indicspecies_aligned_plots/plot_indicspecies_aligned.py")
if( !plotIndicspeciesAlignedScriptFile.exists() ) {
    exit 1, "plot_indicspecies_aligned.py not found in project directory"
}
def plotIndicspeciesAlignedScriptPath = plotIndicspeciesAlignedScriptFile.canonicalPath
def clustermapsScriptFile = new File("${projectDir}/processes/clustermaps/plot_clustermaps.py")
if( !clustermapsScriptFile.exists() ) {
    exit 1, "plot_clustermaps.py not found in project directory"
}
def clustermapsScriptPath = clustermapsScriptFile.canonicalPath
def spieceasiScriptFile = new File("${projectDir}/processes/spieceasi/run_spieceasi.R")
if( !spieceasiScriptFile.exists() ) {
    exit 1, "run_spieceasi.R not found in project directory"
}
def spieceasiScriptPath = spieceasiScriptFile.canonicalPath
def networkModulesScriptFile = new File("${projectDir}/processes/network_modules/network_modules.R")
if( !networkModulesScriptFile.exists() ) {
    exit 1, "network_modules.R not found in project directory"
}
def networkModulesScriptPath = networkModulesScriptFile.canonicalPath
def graphNetworkScriptFile = new File("${projectDir}/processes/graph_network/graph_network.py")
if( !graphNetworkScriptFile.exists() ) {
    exit 1, "graph_network.py not found in project directory"
}
def graphNetworkScriptPath = graphNetworkScriptFile.canonicalPath
def masterSummaryScriptFile = new File("${projectDir}/processes/master_summary/build_master_asv_summary.py")
if( !masterSummaryScriptFile.exists() ) {
    exit 1, "summary/build_master_asv_summary.py not found in project directory"
}
def masterSummaryScriptPath = masterSummaryScriptFile.canonicalPath
def asvMagLinkScriptFile = new File("${projectDir}/processes/asv_mag_link/asv_mag_barrnap_linker.py")
if( !asvMagLinkScriptFile.exists() ) {
    exit 1, "asv_mag_barrnap_linker.py not found in project directory"
}
def asvMagLinkScriptPath = asvMagLinkScriptFile.canonicalPath
def plotAsvMagLinkScriptFile = new File("${projectDir}/processes/asv_mag_link/plot_asv_mag_link.py")
if( !plotAsvMagLinkScriptFile.exists() ) {
    exit 1, "plot_asv_mag_link.py not found in project directory"
}
def plotAsvMagLinkScriptPath = plotAsvMagLinkScriptFile.canonicalPath
def asvMagNetworkScriptFile = new File("${projectDir}/processes/asv_mag_network/asv_mag_network.py")
if( !asvMagNetworkScriptFile.exists() ) {
    exit 1, "asv_mag_network.py not found in project directory"
}
def asvMagNetworkScriptPath = asvMagNetworkScriptFile.canonicalPath
def asvMagNetworkScriptHash = fileMd5(asvMagNetworkScriptFile)
def moduleMagAnchorsScriptFile = new File("${projectDir}/processes/module_mag_anchors/summarize_module_mag_anchors.py")
if( !moduleMagAnchorsScriptFile.exists() ) {
    exit 1, "summarize_module_mag_anchors.py not found in project directory"
}
def moduleMagAnchorsScriptPath = moduleMagAnchorsScriptFile.canonicalPath
def powerAnalysisScriptFile = new File("${projectDir}/processes/power_analysis_pipeline/run_power_analysis_pipeline.sh")
if( !powerAnalysisScriptFile.exists() ) {
    exit 1, "run_power_analysis_pipeline.sh not found in project directory"
}
def powerAnalysisScriptPath = powerAnalysisScriptFile.canonicalPath
def brayPatientAwareScriptFile = new File("${projectDir}/processes/bray_patient_aware/run_bray_permanova_patient_aware.R")
if( !brayPatientAwareScriptFile.exists() ) {
    exit 1, "run_bray_permanova_patient_aware.R not found in project directory"
}
def brayPatientAwareScriptPath = brayPatientAwareScriptFile.canonicalPath
def plotBrayPatientAwareScriptFile = new File("${projectDir}/processes/bray_patient_aware/plot_bray_permanova_patient_aware.py")
if( !plotBrayPatientAwareScriptFile.exists() ) {
    exit 1, "plot_bray_permanova_patient_aware.py not found in project directory"
}
def plotBrayPatientAwareScriptPath = plotBrayPatientAwareScriptFile.canonicalPath
def taxonomicAbundanceObservedScriptFile = new File("${projectDir}/processes/taxonomy_patient_aware/run_taxonomic_abundance_analysis.py")
if( !taxonomicAbundanceObservedScriptFile.exists() ) {
    exit 1, "run_taxonomic_abundance_analysis.py not found in project directory"
}
def taxonomicAbundanceObservedScriptPath = taxonomicAbundanceObservedScriptFile.canonicalPath
def taxonomicSampleTypeObservedScriptFile = new File("${projectDir}/processes/taxonomy_patient_aware/run_taxonomic_sample_type_analysis.py")
if( !taxonomicSampleTypeObservedScriptFile.exists() ) {
    exit 1, "run_taxonomic_sample_type_analysis.py not found in project directory"
}
def taxonomicSampleTypeObservedScriptPath = taxonomicSampleTypeObservedScriptFile.canonicalPath
def plotTaxonomicObservedScriptFile = new File("${projectDir}/processes/taxonomy_patient_aware/plot_taxonomic_observed_analysis.py")
if( !plotTaxonomicObservedScriptFile.exists() ) {
    exit 1, "plot_taxonomic_observed_analysis.py not found in project directory"
}
def plotTaxonomicObservedScriptPath = plotTaxonomicObservedScriptFile.canonicalPath
def prepareLungStatusScriptFile = new File("${projectDir}/processes/lung_status_analysis/prepare_lung_status_data.py")
if( !prepareLungStatusScriptFile.exists() ) {
    exit 1, "prepare_lung_status_data.py not found in project directory"
}
def prepareLungStatusScriptPath = prepareLungStatusScriptFile.canonicalPath
def lungStatusAnalysisScriptFile = new File("${projectDir}/processes/lung_status_analysis/run_lung_status_analysis.R")
if( !lungStatusAnalysisScriptFile.exists() ) {
    exit 1, "run_lung_status_analysis.R not found in project directory"
}
def lungStatusAnalysisScriptPath = lungStatusAnalysisScriptFile.canonicalPath
def plotLungStatusScriptFile = new File("${projectDir}/processes/lung_status_analysis/plot_lung_status_analysis.py")
if( !plotLungStatusScriptFile.exists() ) {
    exit 1, "plot_lung_status_analysis.py not found in project directory"
}
def plotLungStatusScriptPath = plotLungStatusScriptFile.canonicalPath
def plotVocCorrScriptFile = new File("${projectDir}/processes/voc_correlation/plot_voc_corr.py")
if( !plotVocCorrScriptFile.exists() ) {
    exit 1, "plot_voc_corr.py not found in project directory"
}
def plotVocCorrScriptPath = plotVocCorrScriptFile.canonicalPath
def plotVocCorrScriptHash = fileMd5(plotVocCorrScriptFile)
def measurementAssociationScriptFile = new File("${projectDir}/processes/measurement_association/measurement_association.py")
if( !measurementAssociationScriptFile.exists() ) {
    exit 1, "measurement_association.py not found in project directory"
}
def measurementAssociationScriptPath = measurementAssociationScriptFile.canonicalPath
def measurementAssociationScriptHash = fileMd5(measurementAssociationScriptFile)
def measurementAssociationRScriptFile = new File("${projectDir}/processes/measurement_association/run_measurement_association.R")
if( !measurementAssociationRScriptFile.exists() ) {
    exit 1, "run_measurement_association.R not found in project directory"
}
def measurementAssociationRScriptPath = measurementAssociationRScriptFile.canonicalPath
def measurementAssociationRScriptHash = fileMd5(measurementAssociationRScriptFile)
def groupingDiagnosticsScriptFile = new File("${projectDir}/processes/grouping_diagnostics/grouping_diagnostics.py")
if( !groupingDiagnosticsScriptFile.exists() ) {
    exit 1, "grouping_diagnostics.py not found in project directory"
}
def groupingDiagnosticsScriptPath = groupingDiagnosticsScriptFile.canonicalPath
def groupingDiagnosticsScriptHash = fileMd5(groupingDiagnosticsScriptFile)
def groupLabelAugmentationScriptFile = new File("${projectDir}/processes/group_label_augmentation/group_label_augmentation.py")
if( !groupLabelAugmentationScriptFile.exists() ) {
    exit 1, "group_label_augmentation.py not found in project directory"
}
def groupLabelAugmentationScriptPath = groupLabelAugmentationScriptFile.canonicalPath
def groupLabelAugmentationScriptHash = fileMd5(groupLabelAugmentationScriptFile)
def emptyModulesScriptFile = new File("${projectDir}/processes/master_summary/empty_modules.tsv")
if( !emptyModulesScriptFile.exists() ) {
    exit 1, "empty_modules.tsv not found in project directory"
}
def emptyModulesPath = emptyModulesScriptFile.canonicalPath
def sinaConfig = config.sina ?: [:]
def sinaDownloadSubdir = (sinaConfig.download_subdir ?: 'sina_reference').toString()
def sinaDownloadDir = new File(outputDir, sinaDownloadSubdir)
sinaDownloadDir.mkdirs()
def defaultSinaReferenceFile = new File(sinaDownloadDir, sinaReferenceFilename)
def configuredSinaReference = sinaConfig.reference ? resolveOptionalPath(sinaConfig.reference, configRoot) : null
def initialSinaReferencePath = configuredSinaReference ?: defaultSinaReferenceFile.canonicalPath
File sinaReferenceFile = new File(initialSinaReferencePath)
if( !sinaReferenceFile.exists() ) {
    if( configuredSinaReference && sinaReferenceFile.canonicalPath != defaultSinaReferenceFile.canonicalPath ) {
        log.warn "Configured SINA reference not found at ${sinaReferenceFile.canonicalPath}; downloading to ${defaultSinaReferenceFile.canonicalPath}."
        sinaReferenceFile = defaultSinaReferenceFile
    }
    def referenceUrl = sinaConfig.reference_url ?: defaultSinaReferenceUrl
    log.info "Downloading SILVA reference (${sinaReferenceFilename}) from ${referenceUrl}"
    downloadReference(referenceUrl, sinaReferenceFile)
}
if( !sinaReferenceFile.exists() ) {
    exit 1, "Failed to obtain SINA reference file at ${sinaReferenceFile}"
}
def sinaReferencePath = sinaReferenceFile.canonicalPath
def sinaRegionsRaw = sinaConfig.regions
List<String> sinaRegionList
if( sinaRegionsRaw instanceof List ) {
    sinaRegionList = sinaRegionsRaw.collect { it.toString().trim() }.findAll { it }
} else if( sinaRegionsRaw ) {
    sinaRegionList = sinaRegionsRaw.toString().split(/[,|]/).collect { it.trim() }.findAll { it }
} else {
    sinaRegionList = ['V4']
}
if( !sinaRegionList ) {
    sinaRegionList = ['V4']
}
def sinaRegionsArg = sinaRegionList.join(',')
def sinaTrimTarget = sinaConfig.trim_to ?: sinaRegionList[0]
def sinaBatchSize = sinaConfig.batch_size ? (sinaConfig.batch_size as int) : 1000000
def sinaKeepGaps = (sinaConfig.keep_gaps ?: false) as boolean
def sinaVerbose = (sinaConfig.verbose ?: false) as boolean
def sinaIdColumn = sinaConfig.id_column ?: 'ASV_ID'
def sinaThreads = sinaConfig.threads ? (sinaConfig.threads as int) : pipelineThreads
def generalStatsConfig = config.general_stats ?: [:]
def generalStatsEnabled = generalStatsConfig.containsKey('enabled') ? (generalStatsConfig.enabled as boolean) : true
def rawInputFiles = generalStatsEnabled ? sampleRecords.collectMany { rec ->
    def list = [rec.r1]
    if( rec.paired && rec.r2 ) {
        list << rec.r2
    }
    return list
}.unique() : []
def fastpOutputFiles = generalStatsEnabled ? sampleRecords.collectMany { rec ->
    def outputs = [
        new File(dirMap.fastp, "${rec.sample_id}_R1.fastq.gz").absolutePath
    ]
    outputs << new File(dirMap.fastp, "${rec.sample_id}_R2.fastq.gz").absolutePath
    return outputs
} : []
def filteredOutputFiles = generalStatsEnabled ? sampleRecords.collect { rec ->
    new File(dirMap.filter, "${rec.sample_id}.filtered.fasta.gz").absolutePath
} : []
def generalStatsRawArgs = joinShellArgs(rawInputFiles)
def generalStatsFastpArgs = joinShellArgs(fastpOutputFiles)
def generalStatsFilteredArgs = joinShellArgs(filteredOutputFiles)
def taxonomyScriptFile = new File("${projectDir}/processes/taxonomy/qiime_vs_classifier.py")
if( !taxonomyScriptFile.exists() ) {
    exit 1, "qiime_vs_classifier.py not found in project directory"
}
def taxonomyScriptPath = taxonomyScriptFile.canonicalPath
def taxonomyConfig = config.taxonomy ?: [:]
def taxonomyDownloadSubdir = (taxonomyConfig.download_subdir ?: 'taxonomy_reference').toString()
def taxonomyDownloadDir = new File(outputDir, taxonomyDownloadSubdir)
taxonomyDownloadDir.mkdirs()
def taxonomyTaxFilename = taxonomyConfig.ref_taxonomy_filename ?: defaultTaxonomyTaxFilename
def taxonomySeqFilename = taxonomyConfig.ref_sequences_filename ?: defaultTaxonomySeqsFilename

def taxonomyRefTaxonomyResolved = taxonomyConfig.ref_taxonomy ? resolveOptionalPath(taxonomyConfig.ref_taxonomy, configRoot) : null
def taxonomyRefTaxonomyFile = taxonomyRefTaxonomyResolved ? new File(taxonomyRefTaxonomyResolved) : new File(taxonomyDownloadDir, taxonomyTaxFilename)
if( !taxonomyRefTaxonomyFile.exists() ) {
    def taxonomyUrl = taxonomyConfig.ref_taxonomy_url ?: defaultTaxonomyTaxUrl
    if( !taxonomyUrl ) {
        exit 1, "taxonomy.ref_taxonomy or taxonomy.ref_taxonomy_url must be provided to obtain SILVA taxonomy artifact"
    }
    log.info "Downloading SILVA taxonomy artifact from ${taxonomyUrl}"
    downloadReference(taxonomyUrl, taxonomyRefTaxonomyFile)
}
if( !taxonomyRefTaxonomyFile.exists() ) {
    exit 1, "Taxonomy reference taxonomy file not found: ${taxonomyRefTaxonomyFile}"
}

def taxonomyRefSequencesResolved = taxonomyConfig.ref_sequences ? resolveOptionalPath(taxonomyConfig.ref_sequences, configRoot) : null
def taxonomyRefSequencesFile = taxonomyRefSequencesResolved ? new File(taxonomyRefSequencesResolved) : new File(taxonomyDownloadDir, taxonomySeqFilename)
if( !taxonomyRefSequencesFile.exists() ) {
    def taxonomySeqsUrl = taxonomyConfig.ref_sequences_url ?: defaultTaxonomySeqsUrl
    if( !taxonomySeqsUrl ) {
        exit 1, "taxonomy.ref_sequences or taxonomy.ref_sequences_url must be provided to obtain SILVA sequences artifact"
    }
    log.info "Downloading SILVA sequences artifact from ${taxonomySeqsUrl}"
    downloadReference(taxonomySeqsUrl, taxonomyRefSequencesFile)
}
if( !taxonomyRefSequencesFile.exists() ) {
    exit 1, "Taxonomy reference sequences file not found: ${taxonomyRefSequencesFile}"
}

def taxonomyRefTaxonomy = taxonomyRefTaxonomyFile.canonicalPath
def taxonomyRefSequences = taxonomyRefSequencesFile.canonicalPath
def taxonomyOutputName = taxonomyConfig.output_tsv ?: 'ASV_SILVA_tax.full-length.vsearch.tsv'
def taxonomyStatsName = taxonomyConfig.stats_tsv ?: 'ASV_SILVA_stats.full-length.vsearch.tsv'
def taxonomyUppercaseName = taxonomyConfig.uppercase_fasta ?: 'ASVs.upper.fasta'
def taxonomyUppercasePlainName = taxonomyUppercaseName.toString().endsWith('.gz') ? taxonomyUppercaseName.toString()[0..-4] : taxonomyUppercaseName.toString()
def taxonomyUppercaseGzName = taxonomyUppercaseName.toString().endsWith('.gz') ? taxonomyUppercaseName.toString() : "${taxonomyUppercasePlainName}.gz"
def taxonomyThreads = taxonomyConfig.threads ? (taxonomyConfig.threads as int) : pipelineThreads
def mitoConfig = config.mito ?: [:]
def mitoEnabled = mitoConfig.containsKey('enabled') ? (mitoConfig.enabled as boolean) : true
def defaultMitoChunkDir = new File(dirMap.asv, "chunks").canonicalPath
def mitoChunkDirPath = mitoConfig.chunk_dir ? resolveOutputRelative(mitoConfig.chunk_dir.toString(), outputDir) : defaultMitoChunkDir
def defaultMitoOutputDir = new File(dirMap.mito, "mitomap").canonicalPath
def mitoOutputDirPath = mitoConfig.output_dir ? resolveOutputRelative(mitoConfig.output_dir.toString(), outputDir) : defaultMitoOutputDir
def mitoBlastDbPath = resolveOptionalPath(mitoConfig.mito_db ?: 'ref_db/mito_ncbi', configRoot)
def mitoBiofDbPath = resolveOptionalPath(mitoConfig.biof_db ?: 'ref_db/ssu_pipeline_contaminants', configRoot)
def mitoBlastFastaPath = mitoConfig.mito_fasta ? resolveOptionalPath(mitoConfig.mito_fasta.toString(), configRoot) : null
def mitoBiofFastaPath = mitoConfig.contaminant_fasta ? resolveOptionalPath(mitoConfig.contaminant_fasta.toString(), configRoot) :
    (mitoConfig.biof_fasta ? resolveOptionalPath(mitoConfig.biof_fasta.toString(), configRoot) : null)
def mitoChunkSize = mitoConfig.chunk_size ? (mitoConfig.chunk_size as int) : 10
def mitomasterWorkers = mitoConfig.mitomaster_workers ? (mitoConfig.mitomaster_workers as int) : 8
def mitomasterRetries = mitoConfig.mitomaster_retries ? (mitoConfig.mitomaster_retries as int) : 4
def mitomasterTimeout = mitoConfig.mitomaster_timeout ? (mitoConfig.mitomaster_timeout as int) : 90
def mitomasterHeaderMode = mitoConfig.mitomaster_header_mode ?: 'first'
def mitoRunMitomaster = mitoConfig.containsKey('run_mitomaster') ? (mitoConfig.run_mitomaster as boolean) : true
def mitoBlastThreads = mitoConfig.blast_threads ? (mitoConfig.blast_threads as int) : pipelineThreads
def mitoPrefix = mitoConfig.prefix ?: 'nontarget'
def mitoFormats = mitoConfig.formats ?: 'svg,pdf'
def mitoMinPident = mitoConfig.min_pident != null ? (mitoConfig.min_pident as double) : 97.0
def mitoMinPercov = mitoConfig.min_percov != null ? (mitoConfig.min_percov as double) : 51.0
def mitoMitoSubstring = mitoConfig.mitochondria_substring ?: 'mitochondria'
def mitoFeatureCol = mitoConfig.feature_col ?: 'Feature ID'
def mitoTaxonCol = mitoConfig.taxon_col ?: 'Taxon'
def mitoConsensusCol = mitoConfig.consensus_col ?: 'Consensus'
def mitoSteps = mitoConfig.steps ?: 'BioFactorial,Qiime_NB_FULL,MITOMASTER,BLAST_mito'
def mitoHostFirstStep = mitoConfig.host_first_step ?: 'BioFactorial'
def mitoFigsize = mitoConfig.figsize ?: '10x6'
def mitoStyle = mitoConfig.style ?: 'whitegrid'
def mitoDpi = mitoConfig.dpi ? (mitoConfig.dpi as int) : 300
def mitoNoPlots = (mitoConfig.no_plots ?: false) as boolean
def filterCountsConfig = config.filter_counts ?: [:]
def filterCountsEnabled = filterCountsConfig.containsKey('enabled') ? (filterCountsConfig.enabled as boolean) : true
if( filterCountsEnabled && !mitoEnabled ) {
    exit 1, "filter_counts.enabled requires mito.enabled to be true"
}
def filterCountsMetadataPath = filterCountsConfig.metadata ? resolveOptionalPath(filterCountsConfig.metadata, configRoot) : null
if( filterCountsEnabled && filterCountsMetadataPath && !new File(filterCountsMetadataPath).exists() ) {
    exit 1, "filter_counts.metadata not found: ${filterCountsMetadataPath}"
}
def filterCountsOutputName = filterCountsConfig.output ?: 'ASV_target.tsv'
def filterCountsGroupCol = filterCountsConfig.group_col ?: 'Depth'
def filterCountsMinGroup = filterCountsConfig.min_group_size ? (filterCountsConfig.min_group_size as int) : 3
def filterCountsAbundance = filterCountsConfig.abundance_threshold != null ? (filterCountsConfig.abundance_threshold as double) : 0.005d
def filterCountsSampleCol = filterCountsConfig.sample_id_col ?: 'longID'
def filterCountsMinConsensus = filterCountsConfig.min_consensus != null ? (filterCountsConfig.min_consensus as double) : 0d
def filterCountsTaxonCol = filterCountsConfig.taxon_col ?: 'Taxon'
def filterCountsConsensusCol = filterCountsConfig.consensus_col ?: 'Consensus'
def filterCountsBiofactorialCol = filterCountsConfig.biofactorial_col ?: 'BioFactorial'
def filterCountsMitoColsRaw = filterCountsConfig.mito_cols
List<String> filterCountsMitoCols
if( filterCountsMitoColsRaw instanceof List ) {
    filterCountsMitoCols = filterCountsMitoColsRaw.collect { it.toString() }
} else if( filterCountsMitoColsRaw ) {
    filterCountsMitoCols = filterCountsMitoColsRaw.toString().split(/[,|]/).collect { it.trim() }.findAll { it }
} else {
    filterCountsMitoCols = ['MITOMASTER','BLAST_mito']
}
def filterCountsExcludeTaxaRaw = filterCountsConfig.exclude_taxa
List<String> filterCountsExcludeTaxa = []
if( filterCountsExcludeTaxaRaw instanceof List ) {
    filterCountsExcludeTaxa = filterCountsExcludeTaxaRaw.collect { item ->
        if( item instanceof Map ) {
            def rank = item.rank ?: item.level
            def value = item.value ?: item.taxon ?: item.name
            (rank && value) ? "${rank}:${value}".toString() : ''
        } else {
            item.toString()
        }
    }.findAll { it?.trim() }
} else if( filterCountsExcludeTaxaRaw instanceof Map ) {
    filterCountsExcludeTaxaRaw.each { rank, values ->
        if( values instanceof List ) {
            values.each { value -> filterCountsExcludeTaxa << "${rank}:${value}".toString() }
        } else if( values ) {
            filterCountsExcludeTaxa << "${rank}:${values}".toString()
        }
    }
} else if( filterCountsExcludeTaxaRaw ) {
    filterCountsExcludeTaxa = filterCountsExcludeTaxaRaw.toString().split(/[,|]/).collect { it.trim() }.findAll { it }
}
def filterCountsSaveIntermediates = (filterCountsConfig.save_intermediates ?: false) as boolean
def defaultFilterMitoDir = new File(dirMap.mito, "ASVs").canonicalPath
def filterCountsMitoDir = filterCountsConfig.mito_output_dir ? resolveOutputRelative(filterCountsConfig.mito_output_dir.toString(), outputDir) : defaultFilterMitoDir
if( mitoEnabled ) {
    ensureBlastReferenceExists(mitoBlastDbPath, mitoBlastFastaPath, 'mitochondrial')
    ensureBlastReferenceExists(mitoBiofDbPath, mitoBiofFastaPath, 'contaminant')
    new File(mitoChunkDirPath).parentFile?.mkdirs()
    new File(mitoOutputDirPath).mkdirs()
}
def sankeyConfig = config.sankey ?: [:]
def sankeyEnabled = (sankeyConfig.enabled ?: false) as boolean
def sankeyMetadataPath = sankeyConfig.metadata ? resolveOptionalPath(sankeyConfig.metadata, configRoot) : resolveOptionalPath("ref_db/asv_cruise_metadata.tsv", configRoot)
if( sankeyEnabled && (!sankeyMetadataPath || !new File(sankeyMetadataPath).exists()) ) {
    exit 1, "Sankey metadata file not found: ${sankeyMetadataPath}"
}
def sankeySubDir = sankeyConfig.sub_dir ?: '.'
def sankeySampCol = sankeyConfig.sample_col ?: 'lmp_id'
def sankeyGroupCol = sankeyConfig.group1_col ?: 'group1'
def sankeyColorCol = sankeyConfig.color_col ?: 'Color'
if( sankeyEnabled ) {
    def sankeyPalettePath = sankeyConfig.palette_file ? resolveOptionalPath(sankeyConfig.palette_file.toString(), configRoot) : null
    def sankeyAssets = prepareMetadataAssets(
        sankeyMetadataPath,
        sankeySampCol.toString(),
        sankeyGroupCol.toString(),
        sankeyColorCol.toString(),
        sankeyPalettePath,
        new File(dirMap.metadata, "sankey_${safeFilename(sankeyGroupCol.toString())}")
    )
    sankeyMetadataPath = sankeyAssets.metadata
    log.info "Sankey metadata palette: ${sankeyAssets.palette}"
}
def sankeyKeepTypesRaw = sankeyConfig.keep_types
List<String> sankeyKeepTypes = []
if( sankeyKeepTypesRaw instanceof List ) {
    sankeyKeepTypes = sankeyKeepTypesRaw.collect { it.toString().trim() }.findAll { it }
} else if( sankeyKeepTypesRaw ) {
    sankeyKeepTypes = sankeyKeepTypesRaw.toString().split(/[,|]/).collect { it.trim() }.findAll { it }
}
def sankeyVerticalOrderRaw = sankeyConfig.vertical_order ?: sankeyConfig.group_order
List<String> sankeyVerticalOrder = normalizePresetList(sankeyVerticalOrderRaw, [], config.order_presets ?: [:])
def sankeyOutputPrefix = sankeyConfig.output_prefix ?: "metadata/data_loss_sankey"
def sankeyTitle = sankeyConfig.title ?: "Data Loss Flow"
def sankeyMakeLabeled = (sankeyConfig.make_labeled == null) ? true : (sankeyConfig.make_labeled as boolean)
def sankeyMakeUnlabeled = (sankeyConfig.make_unlabeled == null) ? true : (sankeyConfig.make_unlabeled as boolean)
def sankeyArrangement = (sankeyConfig.arrangement ?: 'snap').toString().trim().toLowerCase()
if( !(sankeyArrangement in ['snap', 'perpendicular', 'freeform', 'fixed']) ) {
    exit 1, "Invalid sankey.arrangement '${sankeyArrangement}'. Allowed: snap, perpendicular, freeform, fixed"
}
if( sankeyEnabled && filterCountsEnabled && !filterCountsSaveIntermediates ) {
    exit 1, "Sankey requires filter_counts.save_intermediates to be true to access intermediate tables"
}
if( sankeyEnabled && !filterCountsEnabled ) {
    exit 1, "Sankey requires filter_counts.enabled to be true"
}
if( sankeyEnabled && !generalStatsEnabled ) {
    exit 1, "Sankey requires general_stats.enabled to be true"
}
def analysisCfg = parseMetadataAndBasicAnalysisConfig(config, configRoot, outputDir, filterCountsEnabled as boolean, generalStatsEnabled as boolean, pipelineThreads as int, dirMap)
analysisCfg.each { key, value -> binding.setVariable(key as String, value) }

def indicatorCfg = parseIndicatorAndNetworkConfig(config, configRoot, outputDir, pipelineThreads as int, analysisCfg)
indicatorCfg.each { key, value -> binding.setVariable(key as String, value) }

def parseMetadataAndBasicAnalysisConfig(config, File configRoot, String outputDir, boolean filterCountsEnabled, boolean generalStatsEnabled, int pipelineThreads, Map dirMap) {
    def metadataPlotsConfig = config.metadata_plots ?: [:]
    boolean metadataPlotsEnabled = metadataPlotsConfig.containsKey('enabled') ? (metadataPlotsConfig.enabled as boolean) : true
    if( metadataPlotsEnabled && !filterCountsEnabled ) {
        exit 1, "metadata_plots.enabled requires filter_counts.enabled to be true"
    }
    if( metadataPlotsEnabled && !generalStatsEnabled ) {
        exit 1, "metadata_plots.enabled requires general_stats.enabled to be true"
    }
    def metadataPlotsMetadataPath = metadataPlotsConfig.metadata ? resolveOptionalPath(metadataPlotsConfig.metadata, configRoot) : resolveOptionalPath("ref_db/asv_cruise_metadata.tsv", configRoot)
    if( metadataPlotsEnabled && (!metadataPlotsMetadataPath || !new File(metadataPlotsMetadataPath).exists()) ) {
        exit 1, "metadata_plots metadata file not found: ${metadataPlotsMetadataPath}"
    }
    def metadataPlotsSubDir = metadataPlotsConfig.sub_dir ?: '.'
    def metadataPlotsSampleCol = metadataPlotsConfig.sample_col ?: 'sampleID'
    def metadataPlotsTypeCol = metadataPlotsConfig.type_col ?: (metadataPlotsConfig.group1_col ?: 'Depth')
    def metadataPlotsColorCol = metadataPlotsConfig.color_col ?: 'Color'
    if( metadataPlotsEnabled ) {
        def configuredPalettePath = metadataPlotsConfig.palette_file ? resolveOptionalPath(metadataPlotsConfig.palette_file.toString(), configRoot) : null
        def metadataAssets = prepareMetadataAssets(
            metadataPlotsMetadataPath,
            metadataPlotsSampleCol.toString(),
            metadataPlotsTypeCol.toString(),
            metadataPlotsColorCol.toString(),
            configuredPalettePath,
            new File(dirMap.metadata, safeFilename(metadataPlotsTypeCol.toString()))
        )
        metadataPlotsMetadataPath = metadataAssets.metadata
        log.info "Metadata palette: ${metadataAssets.palette}"
    }
    def metadataPlotsBiochemAssignmentsPath = metadataPlotsConfig.biochem_assignments ? resolveOptionalPath(metadataPlotsConfig.biochem_assignments, configRoot) : null
    def metadataPlotsBiochemSampleCol = metadataPlotsConfig.biochem_sample_col ?: 'cruise_year_month_depth'
    def metadataPlotsStratificationTimeseriesPath = metadataPlotsConfig.stratification_timeseries ? resolveOptionalPath(metadataPlotsConfig.stratification_timeseries, configRoot) : null
    def metadataPlotsStratMetaJoinCol = metadataPlotsConfig.strat_meta_join_col ?: 'Cruise'
    def metadataPlotsStratJoinCol = metadataPlotsConfig.strat_join_col ?: 'Cruise'
    def metadataGroupNormalizationConfig = metadataPlotsConfig.group_normalization instanceof Map ? metadataPlotsConfig.group_normalization : [:]
    boolean metadataGroupNormalizationEnabled = metadataGroupNormalizationConfig.containsKey('enabled') ? (metadataGroupNormalizationConfig.enabled as boolean) : false
    def metadataGroupNormalizationColsRaw = metadataGroupNormalizationConfig.columns ?: []
    List<String> metadataGroupNormalizationCols = metadataGroupNormalizationColsRaw instanceof List ?
        metadataGroupNormalizationColsRaw.collect { it.toString().trim() }.findAll { it } :
        metadataGroupNormalizationColsRaw.toString().split(/[,|]/).collect { it.trim() }.findAll { it }
    def metadataGroupNormalizationPattern = metadataGroupNormalizationConfig.pattern ? metadataGroupNormalizationConfig.pattern.toString() : ''
    def metadataGroupNormalizationReplacement = metadataGroupNormalizationConfig.replacement ? metadataGroupNormalizationConfig.replacement.toString().trim() : 'outlier'
    boolean metadataGroupNormalizationPreserveSource = metadataGroupNormalizationConfig.containsKey('preserve_source') ? (metadataGroupNormalizationConfig.preserve_source as boolean) : true
    if( metadataGroupNormalizationEnabled && (metadataGroupNormalizationCols.isEmpty() || !metadataGroupNormalizationPattern) ) {
        exit 1, "metadata_plots.group_normalization requires columns and pattern when enabled"
    }
    def metadataBiochemIncludeRaw = metadataPlotsConfig.biochem_include_cols
    List<String> metadataPlotsBiochemIncludeCols = []
    if( metadataBiochemIncludeRaw instanceof List ) {
        metadataPlotsBiochemIncludeCols = metadataBiochemIncludeRaw.collect { it.toString().trim() }.findAll { it }
    } else if( metadataBiochemIncludeRaw ) {
        metadataPlotsBiochemIncludeCols = metadataBiochemIncludeRaw.toString().split(/[,|]/).collect { it.trim() }.findAll { it }
    }
    def metadataStratIncludeRaw = metadataPlotsConfig.strat_include_cols
    List<String> metadataPlotsStratIncludeCols = []
    if( metadataStratIncludeRaw instanceof List ) {
        metadataPlotsStratIncludeCols = metadataStratIncludeRaw.collect { it.toString().trim() }.findAll { it }
    } else if( metadataStratIncludeRaw ) {
        metadataPlotsStratIncludeCols = metadataStratIncludeRaw.toString().split(/[,|]/).collect { it.trim() }.findAll { it }
    }
    def metadataBiochemMetaJoinRaw = metadataPlotsConfig.biochem_meta_join_cols
    List<String> metadataPlotsBiochemMetaJoinCols = []
    if( metadataBiochemMetaJoinRaw instanceof List ) {
        metadataPlotsBiochemMetaJoinCols = metadataBiochemMetaJoinRaw.collect { it.toString().trim() }.findAll { it }
    } else if( metadataBiochemMetaJoinRaw ) {
        metadataPlotsBiochemMetaJoinCols = metadataBiochemMetaJoinRaw.toString().split(/[,|]/).collect { it.trim() }.findAll { it }
    }
    def metadataBiochemJoinRaw = metadataPlotsConfig.biochem_join_cols
    List<String> metadataPlotsBiochemJoinCols = []
    if( metadataBiochemJoinRaw instanceof List ) {
        metadataPlotsBiochemJoinCols = metadataBiochemJoinRaw.collect { it.toString().trim() }.findAll { it }
    } else if( metadataBiochemJoinRaw ) {
        metadataPlotsBiochemJoinCols = metadataBiochemJoinRaw.toString().split(/[,|]/).collect { it.trim() }.findAll { it }
    }
    if( metadataPlotsBiochemMetaJoinCols.size() != metadataPlotsBiochemJoinCols.size() ) {
        exit 1, "metadata_plots.biochem_meta_join_cols and metadata_plots.biochem_join_cols must have the same number of entries"
    }
    def metadataKeepTypesRaw = metadataPlotsConfig.keep_types
    List<String> metadataKeepTypes = []
    if( metadataKeepTypesRaw instanceof List ) {
        metadataKeepTypes = metadataKeepTypesRaw.collect { it.toString().trim() }.findAll { it }
    } else if( metadataKeepTypesRaw ) {
        metadataKeepTypes = metadataKeepTypesRaw.toString().split(/[,|]/).collect { it.trim() }.findAll { it }
    }
    def metadataPlotsSubtractionCol = metadataPlotsConfig.subtraction_group_col ?: metadataPlotsTypeCol
    def metadataSubtractionGroupsRaw = metadataPlotsConfig.subtraction_groups
    List<String> metadataPlotsSubtractionGroups = []
    if( metadataSubtractionGroupsRaw instanceof List ) {
        metadataPlotsSubtractionGroups = metadataSubtractionGroupsRaw.collect { it.toString().trim() }.findAll { it }
    } else if( metadataSubtractionGroupsRaw ) {
        metadataPlotsSubtractionGroups = metadataSubtractionGroupsRaw.toString().split(/[,|]/).collect { it.trim() }.findAll { it }
    }
    def metadataGroupOrderRaw = metadataPlotsConfig.group_order
    List<String> metadataPlotsGroupOrder = normalizePresetList(metadataGroupOrderRaw, [], config.order_presets ?: [:])
    def metadataIncludeRankRaw = metadataPlotsConfig.include_rank
    List<String> metadataIncludeRank = []
    if( metadataIncludeRankRaw instanceof List ) {
        metadataIncludeRank = metadataIncludeRankRaw.collect { it.toString().trim() }.findAll { it }
    } else if( metadataIncludeRankRaw ) {
        metadataIncludeRank = metadataIncludeRankRaw.toString().split(/[,|]/).collect { it.trim() }.findAll { it }
    }
    def metadataPlotsMitoThreshold = metadataPlotsConfig.mito_threshold_line != null ? (metadataPlotsConfig.mito_threshold_line as double) : 1000d
    boolean metadataPlotsRunMicro = metadataPlotsConfig.containsKey('run_micro') ? (metadataPlotsConfig.run_micro as boolean) : true
    boolean metadataPlotsRunMito = metadataPlotsConfig.containsKey('run_mito') ? (metadataPlotsConfig.run_mito as boolean) : true
    if( metadataPlotsEnabled && !metadataPlotsRunMicro && !metadataPlotsRunMito ) {
        exit 1, "metadata_plots configured to skip both micro and mito outputs; disable metadata_plots.enabled instead."
    }
    boolean metadataForceMicroOnly = metadataPlotsRunMicro && !metadataPlotsRunMito
    boolean metadataForceMitoOnly = metadataPlotsRunMito && !metadataPlotsRunMicro

    def batchCorrectionConfig = config.batch_correction ?: [:]
    boolean batchCorrectionEnabled = metadataPlotsEnabled && (batchCorrectionConfig.containsKey('enabled') ? (batchCorrectionConfig.enabled as boolean) : true)
    def batchCorrectionOutputDir = batchCorrectionConfig.output_dir ?: 'batch_correction'
    def batchCorrectionOutputDirAbs = new File(outputDir, batchCorrectionOutputDir).canonicalPath
    def batchCorrectionBatchCol = batchCorrectionConfig.batch_col ?: 'batch'
    def batchCorrectionSampleIdCol = batchCorrectionConfig.sample_id_col ?: metadataPlotsSampleCol
    def batchCorrectionOrientation = batchCorrectionConfig.asv_orientation ?: 'features_rows'
    def batchBiologicalCovariates = batchCorrectionConfig.biological_covariates ? batchCorrectionConfig.biological_covariates.toString().trim() : ''
    def batchBiologicalColorCols = batchCorrectionConfig.biological_color_col ?: 'Depth'
    def batchColorPaletteCols = batchCorrectionConfig.color_palette_col ?: 'Color'
    def batchBiologicalPalettes = batchCorrectionConfig.biological_palettes instanceof Map ? batchCorrectionConfig.biological_palettes : [:]
    def batchBiologicalPalettesJson = groovy.json.JsonOutput.toJson(batchBiologicalPalettes)
    def batchUmapNeighbors = batchCorrectionConfig.umap_neighbors ? (batchCorrectionConfig.umap_neighbors as int) : 15
    def batchUmapMinDist = batchCorrectionConfig.umap_min_dist != null ? (batchCorrectionConfig.umap_min_dist as double) : 0.1d
    def batchHdbscanMinClusterSize = batchCorrectionConfig.hdbscan_min_cluster_size ? (batchCorrectionConfig.hdbscan_min_cluster_size as int) : 5
    def batchHdbscanMinSamples = batchCorrectionConfig.hdbscan_min_samples != null ? (batchCorrectionConfig.hdbscan_min_samples as int) : null
    def batchHdbscanSelectionMethod = batchCorrectionConfig.hdbscan_selection_method ?: 'eom'
    boolean batchOptimize = (batchCorrectionConfig.optimize_clustering ?: false) as boolean
    def batchTargetClusters = batchCorrectionConfig.target_clusters ?: '3-8'
    def batchNFeaturesPlot = batchCorrectionConfig.n_features_plot ? (batchCorrectionConfig.n_features_plot as int) : 5
    def batchRandomState = batchCorrectionConfig.random_state ? (batchCorrectionConfig.random_state as int) : 42
    def batchConqurMode = batchCorrectionConfig.conqur_mode ?: 'libsize'
    def batchConqurNumCore = batchCorrectionConfig.conqur_num_core ? (batchCorrectionConfig.conqur_num_core as int) : pipelineThreads
    def batchConqurBatchRef = batchCorrectionConfig.conqur_batch_ref ? batchCorrectionConfig.conqur_batch_ref.toString().trim() : ''
    boolean batchConqurLogisticLasso = (batchCorrectionConfig.conqur_logistic_lasso ?: false) as boolean
    def batchConqurQuantileType = batchCorrectionConfig.conqur_quantile_type ?: 'standard'
    boolean batchConqurSimpleMatch = (batchCorrectionConfig.conqur_simple_match ?: false) as boolean
    def batchConqurLambdaQuantile = batchCorrectionConfig.conqur_lambda_quantile ?: '2p/n'
    boolean batchConqurInterplt = (batchCorrectionConfig.conqur_interplt ?: false) as boolean
    def batchConqurDelta = batchCorrectionConfig.conqur_delta != null ? (batchCorrectionConfig.conqur_delta as double) : 0.4999d
    boolean batchConqurAutoInstall = (batchCorrectionConfig.conqur_auto_install ?: false) as boolean
    def batchCorrectionPolicy = batchCorrectionConfig.correction_policy ? batchCorrectionConfig.correction_policy.toString().trim().toLowerCase() : 'auto'
    if( !(batchCorrectionPolicy in ['auto','always','never']) ) {
        exit 1, "batch_correction.correction_policy must be one of: auto, always, never"
    }
    def batchAutoMinSampleRho = batchCorrectionConfig.auto_min_sample_rho != null ? (batchCorrectionConfig.auto_min_sample_rho as double) : 0.85d
    def batchAutoMinBrayRho = batchCorrectionConfig.auto_min_bray_rho != null ? (batchCorrectionConfig.auto_min_bray_rho as double) : 0.75d
    def batchAutoMaxBatchEtaRatio = batchCorrectionConfig.auto_max_batch_eta_ratio != null ? (batchCorrectionConfig.auto_max_batch_eta_ratio as double) : 0.95d
    def batchAutoMinBatchEtaDrop = batchCorrectionConfig.auto_min_batch_eta_drop != null ? (batchCorrectionConfig.auto_min_batch_eta_drop as double) : 0.01d
    def batchAutoMinBioEtaRatio = batchCorrectionConfig.auto_min_bio_eta_ratio != null ? (batchCorrectionConfig.auto_min_bio_eta_ratio as double) : 0.70d

    def outlierConfig = config.outlier_detection ?: [:]
    boolean outlierEnabled = batchCorrectionEnabled && (outlierConfig.containsKey('enabled') ? (outlierConfig.enabled as boolean) : true)
    def outlierOutputDir = outlierConfig.output_dir ?: 'outliers_corrected'
    def outlierOutputDirAbs = new File(outputDir, outlierOutputDir).canonicalPath
    def outlierSampleIdCol = outlierConfig.sample_id_col ?: (outlierConfig.sample_col ?: metadataPlotsSampleCol)
    def outlierGroupColsRaw = outlierConfig.group_cols
    List<String> outlierGroupCols = []
    if( outlierGroupColsRaw instanceof List ) {
        outlierGroupCols = outlierGroupColsRaw.collect { it.toString().trim() }.findAll { it }
    } else if( outlierGroupColsRaw ) {
        outlierGroupCols = outlierGroupColsRaw.toString().split(/[,|]/).collect { it.trim() }.findAll { it }
    }
    if( outlierGroupCols.isEmpty() ) {
        outlierGroupCols = ['none']
    }
    // OUTLIER_CHECKER consumes batch correction's CLR output (samples x ASVs).
    def outlierTransform = outlierConfig.transform ?: 'none'
    def outlierOrientation = outlierConfig.asv_orientation ?: 'samples_rows'
    boolean outlierPreTransformed = outlierConfig.containsKey('pre_transformed') ?
        (outlierConfig.pre_transformed as boolean) : true
    boolean outlierScale = (outlierConfig.scale ?: false) as boolean
    boolean outlierUseIso = (outlierConfig.use_iso ?: false) as boolean
    boolean outlierUseSvm = (outlierConfig.use_svm ?: false) as boolean
    boolean outlierUseHdb = (outlierConfig.use_hdb ?: false) as boolean
    def outlierVoteThreshold = outlierConfig.vote_threshold ? (outlierConfig.vote_threshold as int) : 3
    def outlierIsoContamination = outlierConfig.iso_contamination ?: 'auto'
    def outlierIsoEstimators = outlierConfig.iso_estimators ? (outlierConfig.iso_estimators as int) : 100
    def outlierIsoRandomState = outlierConfig.iso_random_state ? (outlierConfig.iso_random_state as int) : 42
    def outlierSvmKernel = outlierConfig.svm_kernel ?: 'rbf'
    def outlierSvmGamma = outlierConfig.svm_gamma ?: 'scale'
    def outlierSvmNu = outlierConfig.svm_nu ? (outlierConfig.svm_nu as double) : 0.1d
    def outlierHdbMinClusterSize = outlierConfig.hdbscan_min_cluster_size ? (outlierConfig.hdbscan_min_cluster_size as int) : 5
    def outlierHdbMinSamples = outlierConfig.hdbscan_min_samples != null ? (outlierConfig.hdbscan_min_samples as int) : null
    def outlierHdbMetric = outlierConfig.hdbscan_metric ?: 'euclidean'

    def collectorsConfig = config.collectors_curve ?: [:]
    boolean collectorsEnabled = metadataPlotsEnabled && (collectorsConfig.containsKey('enabled') ? (collectorsConfig.enabled as boolean) : true)
    def collectorsSampleCol = collectorsConfig.sample_col ?: metadataPlotsSampleCol
    def collectorsGroupCol = collectorsConfig.group_col ?: (collectorsConfig.group1_col ?: metadataPlotsTypeCol)
    def collectorsColorCol = collectorsConfig.color_col ?: 'Color'
    def collectorsGroupColors = collectorsConfig.group_colors ?: (collectorsConfig.group_palette ?: '')
    def collectorsGroupOrderRaw = collectorsConfig.group_order ?: metadataPlotsGroupOrder
    List<String> collectorsGroupOrder = normalizePresetList(collectorsGroupOrderRaw, [], config.order_presets ?: [:])
    def collectorsPermutations = collectorsConfig.permutations ? (collectorsConfig.permutations as int) : 999
    def collectorsSeed = collectorsConfig.seed ? (collectorsConfig.seed as int) : 42
    def collectorsOutPrefix = collectorsConfig.out_prefix ?: 'metadata/collectors_curve'
    def collectorsOutPrefixAbs = new File(outputDir, collectorsOutPrefix).canonicalPath
    def collectorsTitle = collectorsConfig.title ?: ''
    def collectorsFormats = collectorsConfig.formats ?: 'pdf,svg'
    def collectorsXpad = collectorsConfig.xpad != null ? (collectorsConfig.xpad as double) : 0.5d
    def collectorsMaxCols = collectorsConfig.max_cols ? (collectorsConfig.max_cols as int) : 3
    def collectorsShowPerms = collectorsConfig.show_perms ? (collectorsConfig.show_perms as int) : 10
    def collectorsPresenceThreshold = collectorsConfig.presence_threshold != null ? (collectorsConfig.presence_threshold as double) : 0d

    def plotUpsetConfig = config.plot_upset ?: [:]
    boolean plotUpsetRequested = plotUpsetConfig.containsKey('enabled') ? (plotUpsetConfig.enabled as boolean) : false
    if( plotUpsetRequested && !metadataPlotsEnabled ) {
        exit 1, "plot_upset.enabled requires metadata_plots.enabled to be true"
    }
    boolean plotUpsetEnabled = plotUpsetRequested
    def plotUpsetSubDir = plotUpsetConfig.sub_dir ?: '.'
    def plotUpsetDomain = plotUpsetConfig.domain ?: 'micro'
    def plotUpsetTaxonomyPath = plotUpsetConfig.taxonomy_path ? resolveOptionalPath(plotUpsetConfig.taxonomy_path, configRoot) : null
    def plotUpsetSampleIdCol = plotUpsetConfig.sample_id_col ?: metadataPlotsSampleCol
    def plotUpsetGroupCol = plotUpsetConfig.group_col ?: (plotUpsetConfig.group1_col ?: metadataPlotsTypeCol)
    def plotUpsetColorCol = plotUpsetConfig.color_col ?: metadataPlotsColorCol
    def plotUpsetGroupPalette = plotUpsetConfig.group_palette ?: ''
    def plotUpsetGroupOrderRaw = plotUpsetConfig.group_order ?: metadataPlotsGroupOrder
    List<String> plotUpsetGroupOrder = normalizePresetList(plotUpsetGroupOrderRaw, [], config.order_presets ?: [:])
    def plotUpsetSubsetGroupsRaw = plotUpsetConfig.subset_groups
    List<String> plotUpsetSubsetGroups = []
    if( plotUpsetSubsetGroupsRaw instanceof List ) {
        plotUpsetSubsetGroups = plotUpsetSubsetGroupsRaw.collect { it.toString().trim() }.findAll { it }
    } else if( plotUpsetSubsetGroupsRaw ) {
        plotUpsetSubsetGroups = plotUpsetSubsetGroupsRaw.toString().split(/[,|]/).collect { it.trim() }.findAll { it }
    }
    boolean plotUpsetSkipVenn = plotUpsetConfig.containsKey('skip_venn') ? (plotUpsetConfig.skip_venn as boolean) : true
    def plotUpsetFormats = plotUpsetConfig.formats ?: 'pdf,svg,png'
    def plotUpsetFontSize = plotUpsetConfig.font_size != null ? (plotUpsetConfig.font_size as double) : 12d
    boolean plotUpsetRawOnly = plotUpsetConfig.containsKey('raw_only') ? (plotUpsetConfig.raw_only as boolean) : false
    boolean plotUpsetFinalOnly = plotUpsetConfig.containsKey('final_only') ? (plotUpsetConfig.final_only as boolean) : false
    if( plotUpsetRawOnly && plotUpsetFinalOnly ) {
        exit 1, "plot_upset.raw_only and plot_upset.final_only cannot both be true"
    }

    def bubbleplotterConfig = config.bubbleplotter ?: [:]
    boolean bubbleplotterRequested = bubbleplotterConfig.containsKey('enabled') ? (bubbleplotterConfig.enabled as boolean) : false
    if( bubbleplotterRequested && !metadataPlotsEnabled ) {
        exit 1, "bubbleplotter.enabled requires metadata_plots.enabled to be true"
    }
    boolean bubbleplotterEnabled = bubbleplotterRequested
    def bubbleplotterOutputPrefix = bubbleplotterConfig.output_prefix ?: 'metadata/bubble_plot_asv'
    def bubbleplotterOutputPrefixAbs = new File(outputDir, bubbleplotterOutputPrefix).canonicalPath
    def bubbleplotterOutputDirAbs = (new File(bubbleplotterOutputPrefixAbs).parentFile ?: new File(outputDir)).canonicalPath
    def bubbleplotterFormats = bubbleplotterConfig.formats ?: 'pdf,png,svg'
    def bubbleplotterCountCol = bubbleplotterConfig.count_col ?: 'count'
    def bubbleplotterSampleCol = bubbleplotterConfig.sample_col ?: metadataPlotsSampleCol
    def bubbleplotterDepthCol = bubbleplotterConfig.group1_col ?: (bubbleplotterConfig.depth_col ?: metadataPlotsTypeCol)
    def bubbleplotterColorCol = bubbleplotterConfig.color_col ?: metadataPlotsColorCol
    def bubbleplotterMonthCol = bubbleplotterConfig.group2_col ?: (bubbleplotterConfig.month_col ?: 'Month')
    def bubbleplotterGroup1OrderRaw = bubbleplotterConfig.group1_order ?: metadataPlotsGroupOrder
    List<String> bubbleplotterGroup1Order = normalizePresetList(bubbleplotterGroup1OrderRaw, [], config.order_presets ?: [:])
    def bubbleplotterGroup2OrderRaw = bubbleplotterConfig.group2_order ?: (config.indicspecies?.group2_order ?: '')
    List<String> bubbleplotterGroup2Order = normalizePresetList(bubbleplotterGroup2OrderRaw, [], config.order_presets ?: [:])
    def bubbleplotterFigsize = bubbleplotterConfig.figsize ?: '32,60'
    def bubbleplotterScale = bubbleplotterConfig.bubble_scale != null ? (bubbleplotterConfig.bubble_scale as double) : 10d
    boolean bubbleplotterNoAutoSize = bubbleplotterConfig.containsKey('no_auto_size') ? (bubbleplotterConfig.no_auto_size as boolean) : true

    def umapClusteringConfig = config.umap_clustering ?: [:]
    boolean umapClusteringRequested = umapClusteringConfig.containsKey('enabled') ? (umapClusteringConfig.enabled as boolean) : false
    if( umapClusteringRequested && !metadataPlotsEnabled ) {
        exit 1, "umap_clustering.enabled requires metadata_plots.enabled to be true"
    }
    boolean umapClusteringEnabled = umapClusteringRequested
    def umapClusteringOutputPrefix = umapClusteringConfig.output_prefix ?: 'metadata/umap_clustering'
    def umapClusteringOutputPrefixAbs = new File(outputDir, umapClusteringOutputPrefix).canonicalPath
    def umapClusteringOutputDirAbs = (new File(umapClusteringOutputPrefixAbs).parentFile ?: new File(outputDir)).canonicalPath
    def umapClusteringSampleCol = umapClusteringConfig.sample_col ?: metadataPlotsSampleCol
    def umapClusteringCountCol = umapClusteringConfig.count_col ?: 'count'
    def umapClusteringDepthCol = umapClusteringConfig.group1_col ?: (umapClusteringConfig.depth_col ?: metadataPlotsTypeCol)
    def umapClusteringColorCol = umapClusteringConfig.color_col ?: metadataPlotsColorCol
    def umapClusteringSecondaryCol = umapClusteringConfig.group2_col ?: (umapClusteringConfig.secondary_col ?: (umapClusteringConfig.month_col ?: 'Month'))
    def umapClusteringGroup1OrderRaw = umapClusteringConfig.group1_order ?: metadataPlotsGroupOrder
    List<String> umapClusteringGroup1Order = normalizePresetList(umapClusteringGroup1OrderRaw, [], config.order_presets ?: [:])
    def umapClusteringGroup2OrderRaw = umapClusteringConfig.group2_order ?: (config.indicspecies?.group2_order ?: '')
    List<String> umapClusteringGroup2Order = normalizePresetList(umapClusteringGroup2OrderRaw, [], config.order_presets ?: [:])
    def sharedPaletteConfig = config.indicspecies?.group_palettes instanceof Map ? config.indicspecies.group_palettes : [:]
    def umapClusteringGroup1Palette = umapClusteringConfig.group1_palette ?: (sharedPaletteConfig[umapClusteringDepthCol] ?: '')
    def umapClusteringGroup2Palette = umapClusteringConfig.group2_palette ?: (sharedPaletteConfig[umapClusteringSecondaryCol] ?: '')
    def umapClusteringFormats = umapClusteringConfig.formats ?: 'pdf,png,svg'
    def umapClusteringNormalize = umapClusteringConfig.normalize ?: 'clr'
    def umapClusteringTransform = umapClusteringConfig.transform ?: 'sqrt'
    def umapClusteringNeighbors = umapClusteringConfig.n_neighbors ? (umapClusteringConfig.n_neighbors as int) : 15
    def umapClusteringMinDist = umapClusteringConfig.min_dist != null ? (umapClusteringConfig.min_dist as double) : 0.1d
    def umapClusteringMetric = umapClusteringConfig.umap_metric ?: 'euclidean'
    def umapClusteringHdbscanMetric = umapClusteringConfig.hdbscan_metric ?: 'euclidean'
    def umapClusteringMinClusterSize = umapClusteringConfig.min_cluster_size ? (umapClusteringConfig.min_cluster_size as int) : 10
    def umapClusteringMinSamples = umapClusteringConfig.min_samples ? (umapClusteringConfig.min_samples as int) : 5
    boolean umapClusteringNoScale = (umapClusteringConfig.no_scale ?: false) as boolean
    def umapClusteringRandomState = umapClusteringConfig.random_state ? (umapClusteringConfig.random_state as int) : 42

    def diversityConfig = config.diversity ?: [:]
    boolean diversityRequested = diversityConfig.containsKey('enabled') ? (diversityConfig.enabled as boolean) : false
    if( diversityRequested && !metadataPlotsEnabled ) {
        exit 1, "diversity.enabled requires metadata_plots.enabled to be true"
    }
    boolean diversityEnabled = diversityRequested
    def diversityOutputDir = diversityConfig.output_dir ?: 'diversity'
    def diversityOutputDirAbs = new File(outputDir, diversityOutputDir).canonicalPath
    def diversityMitoOutputDir = diversityConfig.mito_output_dir ?: 'mito/diversity'
    def diversityMitoOutputDirAbs = new File(outputDir, diversityMitoOutputDir).canonicalPath
    def diversityMitoInputPath = diversityConfig.mito_input ? resolveOptionalPath(diversityConfig.mito_input, configRoot) : new File(outputDir, 'mito/ASVs/ASV_target.mito.tsv').canonicalPath
    def diversitySampleCol = diversityConfig.sample_col ?: metadataPlotsSampleCol
    def diversityGroupCol = diversityConfig.group_col ?: (diversityConfig.group1_col ?: metadataPlotsTypeCol)
    def diversityColorCol = diversityConfig.color_col ?: 'Color'
    def diversitySecondaryCol = diversityConfig.group2_col ?: (diversityConfig.secondary_col ?: 'Month')
    def diversityGroupPalette = diversityConfig.group1_palette ?: (sharedPaletteConfig[diversityGroupCol] ?: '')
    def diversitySecondaryPalette = diversityConfig.group2_palette ?: (sharedPaletteConfig[diversitySecondaryCol] ?: '')
    def diversityExcludeGroupsRaw = diversityConfig.exclude_groups
    List<String> diversityExcludeGroups = []
    if( diversityExcludeGroupsRaw instanceof List ) {
        diversityExcludeGroups = diversityExcludeGroupsRaw.collect { it.toString().trim() }.findAll { it }
    } else if( diversityExcludeGroupsRaw ) {
        diversityExcludeGroups = diversityExcludeGroupsRaw.toString().split(/[,|]/).collect { it.trim() }.findAll { it }
    }
    def diversityGroupOrderRaw = diversityConfig.group_order ?: metadataPlotsGroupOrder
    List<String> diversityGroupOrder = normalizePresetList(diversityGroupOrderRaw, [], config.order_presets ?: [:])
    boolean diversityRunMito = diversityConfig.containsKey('run_mito') ? (diversityConfig.run_mito as boolean) : true
    def diversityUmapNeighbors = diversityConfig.umap_neighbors ? (diversityConfig.umap_neighbors as int) : 30
    def diversityUmapMinDist = diversityConfig.umap_min_dist != null ? (diversityConfig.umap_min_dist as double) : 0.01d
    def diversityPermutations = diversityConfig.permanova_perms ? (diversityConfig.permanova_perms as int) : 999
    def diversityRandomState = diversityConfig.random_state ? (diversityConfig.random_state as int) : 42
    def diversityBlockCol = diversityConfig.block_col ? diversityConfig.block_col.toString().trim() : ''
    boolean diversityVerbose = diversityConfig.containsKey('verbose') ? (diversityConfig.verbose as boolean) : true
    def diversityPatientAwareConfig = (diversityConfig.patient_aware instanceof Map) ? diversityConfig.patient_aware : [:]
    boolean diversityPatientAwareEnabled = diversityPatientAwareConfig.containsKey('enabled') ? (diversityPatientAwareConfig.enabled as boolean) : false
    def diversityPatientAwareOutputDir = diversityPatientAwareConfig.output_dir ?: 'patient_aware'
    def diversityPatientAwareOutputDirAbs = new File(diversityOutputDirAbs, diversityPatientAwareOutputDir.toString()).canonicalPath
    def diversityPatientAwareSampleCol = diversityPatientAwareConfig.sample_col ?: diversitySampleCol
    def diversityPatientAwarePatientCol = diversityPatientAwareConfig.patient_col ?
        diversityPatientAwareConfig.patient_col.toString().trim() :
        (diversityBlockCol ?: 'Participant_ID')
    def diversityPatientAwareCaseCol = diversityPatientAwareConfig.case_col ?
        diversityPatientAwareConfig.case_col.toString().trim() : 'Case'
    def diversityPatientAwareTypeCol = diversityPatientAwareConfig.type_col ?: diversityGroupCol
    def diversityPatientAwareSampleTypesRaw = diversityPatientAwareConfig.sample_types ?:
        (diversityGroupOrder ? diversityGroupOrder.join(',') : 'Oral Rinse,BAL,Lung Brush')
    def diversityPatientAwareSampleTypes = diversityPatientAwareSampleTypesRaw instanceof List ?
        diversityPatientAwareSampleTypesRaw.collect { it.toString().trim() }.findAll { it }.join(',') :
        diversityPatientAwareSampleTypesRaw.toString().trim()
    boolean diversityPatientAwareExcludeContralateral = diversityPatientAwareConfig.containsKey('exclude_contralateral_in_cancer') ?
        (diversityPatientAwareConfig.exclude_contralateral_in_cancer as boolean) : true
    def diversityPatientAwareContralateralCol = diversityPatientAwareConfig.contralateral_col ?
        diversityPatientAwareConfig.contralateral_col.toString().trim() : 'lung_status'
    def diversityPatientAwareCancerSiteCol = diversityPatientAwareConfig.cancer_site_col ?
        diversityPatientAwareConfig.cancer_site_col.toString().trim() : 'Cancer_Site'
    def diversityPatientAwareLungSideCol = diversityPatientAwareConfig.lung_side_col ?
        diversityPatientAwareConfig.lung_side_col.toString().trim() : 'lung_code'
    def diversityPatientAwareContralateralValue = diversityPatientAwareConfig.contralateral_value ?
        diversityPatientAwareConfig.contralateral_value.toString().trim() : 'Contralateral'
    def diversityPatientAwareContralateralTypesRaw = diversityPatientAwareConfig.contralateral_sample_types ?: 'Lung Brush,BAL'
    def diversityPatientAwareContralateralTypes = diversityPatientAwareContralateralTypesRaw instanceof List ?
        diversityPatientAwareContralateralTypesRaw.collect { it.toString().trim() }.findAll { it }.join(',') :
        diversityPatientAwareContralateralTypesRaw.toString().trim()
    def diversityPatientAwareTransform = diversityPatientAwareConfig.transform ?
        diversityPatientAwareConfig.transform.toString().trim().toLowerCase() : 'none'
    if( !['none', 'rclr'].contains(diversityPatientAwareTransform) ) {
        exit 1, "diversity.patient_aware.transform must be one of: none, rclr"
    }
    def diversityPatientAwarePermutations = diversityPatientAwareConfig.permutations ?
        (diversityPatientAwareConfig.permutations as int) : 9999
    def diversityPatientAwareSeed = diversityPatientAwareConfig.seed ?
        (diversityPatientAwareConfig.seed as int) : diversityRandomState
    boolean diversityPatientAwareRequireCompleteTypes = diversityPatientAwareConfig.containsKey('require_complete_types') ?
        (diversityPatientAwareConfig.require_complete_types as boolean) : false
    return [
        metadataPlotsMetadataPath: metadataPlotsMetadataPath,
        metadataPlotsSubDir: metadataPlotsSubDir,
        metadataPlotsSampleCol: metadataPlotsSampleCol,
        metadataPlotsTypeCol: metadataPlotsTypeCol,
        metadataPlotsColorCol: metadataPlotsColorCol,
        metadataPlotsBiochemAssignmentsPath: metadataPlotsBiochemAssignmentsPath,
        metadataPlotsBiochemSampleCol: metadataPlotsBiochemSampleCol,
        metadataPlotsStratificationTimeseriesPath: metadataPlotsStratificationTimeseriesPath,
        metadataPlotsStratMetaJoinCol: metadataPlotsStratMetaJoinCol,
        metadataPlotsStratJoinCol: metadataPlotsStratJoinCol,
        metadataPlotsSubtractionCol: metadataPlotsSubtractionCol,
        batchCorrectionOutputDir: batchCorrectionOutputDir,
        batchCorrectionOutputDirAbs: batchCorrectionOutputDirAbs,
        batchCorrectionBatchCol: batchCorrectionBatchCol,
        batchCorrectionSampleIdCol: batchCorrectionSampleIdCol,
        batchCorrectionOrientation: batchCorrectionOrientation,
        batchBiologicalCovariates: batchBiologicalCovariates,
        batchBiologicalColorCols: batchBiologicalColorCols,
        batchColorPaletteCols: batchColorPaletteCols,
        batchBiologicalPalettesJson: batchBiologicalPalettesJson,
        batchUmapNeighbors: batchUmapNeighbors,
        batchUmapMinDist: batchUmapMinDist,
        batchHdbscanMinClusterSize: batchHdbscanMinClusterSize,
        batchHdbscanMinSamples: batchHdbscanMinSamples,
        batchHdbscanSelectionMethod: batchHdbscanSelectionMethod,
        batchTargetClusters: batchTargetClusters,
        batchNFeaturesPlot: batchNFeaturesPlot,
        batchRandomState: batchRandomState,
        batchConqurMode: batchConqurMode,
        batchConqurNumCore: batchConqurNumCore,
        batchConqurBatchRef: batchConqurBatchRef,
        batchConqurQuantileType: batchConqurQuantileType,
        batchConqurLambdaQuantile: batchConqurLambdaQuantile,
        batchConqurDelta: batchConqurDelta,
        batchCorrectionPolicy: batchCorrectionPolicy,
        batchAutoMinSampleRho: batchAutoMinSampleRho,
        batchAutoMinBrayRho: batchAutoMinBrayRho,
        batchAutoMaxBatchEtaRatio: batchAutoMaxBatchEtaRatio,
        batchAutoMinBatchEtaDrop: batchAutoMinBatchEtaDrop,
        batchAutoMinBioEtaRatio: batchAutoMinBioEtaRatio,
        outlierOutputDirAbs: outlierOutputDirAbs,
        outlierSampleIdCol: outlierSampleIdCol,
        outlierTransform: outlierTransform,
        outlierOrientation: outlierOrientation,
        outlierVoteThreshold: outlierVoteThreshold,
        outlierIsoContamination: outlierIsoContamination,
        outlierIsoEstimators: outlierIsoEstimators,
        outlierIsoRandomState: outlierIsoRandomState,
        outlierSvmKernel: outlierSvmKernel,
        outlierSvmGamma: outlierSvmGamma,
        outlierSvmNu: outlierSvmNu,
        outlierHdbMinClusterSize: outlierHdbMinClusterSize,
        outlierHdbMinSamples: outlierHdbMinSamples,
        outlierHdbMetric: outlierHdbMetric,
        collectorsSampleCol: collectorsSampleCol,
        collectorsGroupCol: collectorsGroupCol,
        collectorsColorCol: collectorsColorCol,
        collectorsPermutations: collectorsPermutations,
        collectorsSeed: collectorsSeed,
        collectorsOutPrefixAbs: collectorsOutPrefixAbs,
        collectorsTitle: collectorsTitle,
        collectorsFormats: collectorsFormats,
        collectorsXpad: collectorsXpad,
        collectorsMaxCols: collectorsMaxCols,
        collectorsShowPerms: collectorsShowPerms,
        collectorsPresenceThreshold: collectorsPresenceThreshold,
        plotUpsetSubDir: plotUpsetSubDir,
        plotUpsetDomain: plotUpsetDomain,
        plotUpsetTaxonomyPath: plotUpsetTaxonomyPath,
        plotUpsetSampleIdCol: plotUpsetSampleIdCol,
        plotUpsetGroupCol: plotUpsetGroupCol,
        plotUpsetColorCol: plotUpsetColorCol,
        plotUpsetFormats: plotUpsetFormats,
        plotUpsetFontSize: plotUpsetFontSize,
        bubbleplotterOutputPrefixAbs: bubbleplotterOutputPrefixAbs,
        bubbleplotterOutputDirAbs: bubbleplotterOutputDirAbs,
        bubbleplotterFormats: bubbleplotterFormats,
        bubbleplotterCountCol: bubbleplotterCountCol,
        bubbleplotterSampleCol: bubbleplotterSampleCol,
        bubbleplotterDepthCol: bubbleplotterDepthCol,
        bubbleplotterColorCol: bubbleplotterColorCol,
        bubbleplotterMonthCol: bubbleplotterMonthCol,
        bubbleplotterFigsize: bubbleplotterFigsize,
        bubbleplotterScale: bubbleplotterScale,
        umapClusteringOutputPrefixAbs: umapClusteringOutputPrefixAbs,
        umapClusteringOutputDirAbs: umapClusteringOutputDirAbs,
        umapClusteringSampleCol: umapClusteringSampleCol,
        umapClusteringCountCol: umapClusteringCountCol,
        umapClusteringDepthCol: umapClusteringDepthCol,
        umapClusteringColorCol: umapClusteringColorCol,
        umapClusteringSecondaryCol: umapClusteringSecondaryCol,
        umapClusteringGroup1Palette: umapClusteringGroup1Palette,
        umapClusteringGroup2Palette: umapClusteringGroup2Palette,
        umapClusteringFormats: umapClusteringFormats,
        umapClusteringNormalize: umapClusteringNormalize,
        umapClusteringTransform: umapClusteringTransform,
        umapClusteringNeighbors: umapClusteringNeighbors,
        umapClusteringMinDist: umapClusteringMinDist,
        umapClusteringMetric: umapClusteringMetric,
        umapClusteringHdbscanMetric: umapClusteringHdbscanMetric,
        umapClusteringMinClusterSize: umapClusteringMinClusterSize,
        umapClusteringMinSamples: umapClusteringMinSamples,
        umapClusteringRandomState: umapClusteringRandomState,
        diversityOutputDirAbs: diversityOutputDirAbs,
        diversityMitoOutputDirAbs: diversityMitoOutputDirAbs,
        diversityMitoInputPath: diversityMitoInputPath,
        diversitySampleCol: diversitySampleCol,
        diversityGroupCol: diversityGroupCol,
        diversityColorCol: diversityColorCol,
        diversitySecondaryCol: diversitySecondaryCol,
        diversityGroupPalette: diversityGroupPalette,
        diversitySecondaryPalette: diversitySecondaryPalette,
        diversityUmapNeighbors: diversityUmapNeighbors,
        diversityUmapMinDist: diversityUmapMinDist,
        diversityPermutations: diversityPermutations,
        diversityRandomState: diversityRandomState,
        diversityBlockCol: diversityBlockCol,
        diversityPatientAwareOutputDirAbs: diversityPatientAwareOutputDirAbs,
        diversityPatientAwareSampleCol: diversityPatientAwareSampleCol,
        diversityPatientAwarePatientCol: diversityPatientAwarePatientCol,
        diversityPatientAwareCaseCol: diversityPatientAwareCaseCol,
        diversityPatientAwareTypeCol: diversityPatientAwareTypeCol,
        diversityPatientAwareSampleTypes: diversityPatientAwareSampleTypes,
        diversityPatientAwareContralateralCol: diversityPatientAwareContralateralCol,
        diversityPatientAwareCancerSiteCol: diversityPatientAwareCancerSiteCol,
        diversityPatientAwareLungSideCol: diversityPatientAwareLungSideCol,
        diversityPatientAwareContralateralValue: diversityPatientAwareContralateralValue,
        diversityPatientAwareContralateralTypes: diversityPatientAwareContralateralTypes,
        diversityPatientAwareTransform: diversityPatientAwareTransform,
        diversityPatientAwarePermutations: diversityPatientAwarePermutations,
        diversityPatientAwareSeed: diversityPatientAwareSeed,
        metadataPlotsEnabled: metadataPlotsEnabled,
        metadataPlotsBiochemIncludeCols: metadataPlotsBiochemIncludeCols,
        metadataPlotsStratIncludeCols: metadataPlotsStratIncludeCols,
        metadataPlotsBiochemMetaJoinCols: metadataPlotsBiochemMetaJoinCols,
        metadataPlotsBiochemJoinCols: metadataPlotsBiochemJoinCols,
        metadataGroupNormalizationEnabled: metadataGroupNormalizationEnabled,
        metadataGroupNormalizationCols: metadataGroupNormalizationCols,
        metadataGroupNormalizationPattern: metadataGroupNormalizationPattern,
        metadataGroupNormalizationReplacement: metadataGroupNormalizationReplacement,
        metadataGroupNormalizationPreserveSource: metadataGroupNormalizationPreserveSource,
        metadataKeepTypes: metadataKeepTypes,
        metadataPlotsSubtractionGroups: metadataPlotsSubtractionGroups,
        metadataPlotsGroupOrder: metadataPlotsGroupOrder,
        metadataIncludeRank: metadataIncludeRank,
        batchCorrectionEnabled: batchCorrectionEnabled,
        batchOptimize: batchOptimize,
        batchConqurLogisticLasso: batchConqurLogisticLasso,
        batchConqurSimpleMatch: batchConqurSimpleMatch,
        batchConqurInterplt: batchConqurInterplt,
        batchConqurAutoInstall: batchConqurAutoInstall,
        outlierEnabled: outlierEnabled,
        outlierGroupCols: outlierGroupCols,
        outlierPreTransformed: outlierPreTransformed,
        outlierScale: outlierScale,
        outlierUseIso: outlierUseIso,
        outlierUseSvm: outlierUseSvm,
        outlierUseHdb: outlierUseHdb,
        collectorsEnabled: collectorsEnabled,
        collectorsGroupColors: collectorsGroupColors,
        collectorsGroupOrder: collectorsGroupOrder,
        plotUpsetEnabled: plotUpsetEnabled,
        plotUpsetGroupPalette: plotUpsetGroupPalette,
        plotUpsetGroupOrder: plotUpsetGroupOrder,
        plotUpsetSubsetGroups: plotUpsetSubsetGroups,
        plotUpsetSkipVenn: plotUpsetSkipVenn,
        plotUpsetRawOnly: plotUpsetRawOnly,
        plotUpsetFinalOnly: plotUpsetFinalOnly,
        bubbleplotterEnabled: bubbleplotterEnabled,
        bubbleplotterGroup1Order: bubbleplotterGroup1Order,
        bubbleplotterGroup2Order: bubbleplotterGroup2Order,
        bubbleplotterNoAutoSize: bubbleplotterNoAutoSize,
        umapClusteringEnabled: umapClusteringEnabled,
        umapClusteringGroup1Order: umapClusteringGroup1Order,
        umapClusteringGroup2Order: umapClusteringGroup2Order,
        umapClusteringNoScale: umapClusteringNoScale,
        diversityEnabled: diversityEnabled,
        diversityExcludeGroups: diversityExcludeGroups,
        diversityGroupOrder: diversityGroupOrder,
        diversityRunMito: diversityRunMito,
        diversityVerbose: diversityVerbose,
        diversityPatientAwareEnabled: diversityPatientAwareEnabled,
        diversityPatientAwareExcludeContralateral: diversityPatientAwareExcludeContralateral,
        diversityPatientAwareRequireCompleteTypes: diversityPatientAwareRequireCompleteTypes,
    ]
}

def parseIndicatorAndNetworkConfig(config, File configRoot, String outputDir, int pipelineThreads, Map analysisCfg) {
    def metadataPlotsEnabled = analysisCfg.metadataPlotsEnabled
    def metadataPlotsMetadataPath = analysisCfg.metadataPlotsMetadataPath
    def metadataPlotsSampleCol = analysisCfg.metadataPlotsSampleCol
    def metadataPlotsTypeCol = analysisCfg.metadataPlotsTypeCol
    def metadataPlotsColorCol = analysisCfg.metadataPlotsColorCol
    def metadataPlotsGroupOrder = analysisCfg.metadataPlotsGroupOrder
    def indicspeciesConfig = config.indicspecies ?: [:]
    boolean indicspeciesRequested = indicspeciesConfig.containsKey('enabled') ? (indicspeciesConfig.enabled as boolean) : false
    if( indicspeciesRequested && !metadataPlotsEnabled ) {
        exit 1, "indicspecies.enabled requires metadata_plots.enabled to be true"
    }
    def indicspeciesGroupColsRaw = indicspeciesConfig.group_cols
    List<String> indicspeciesGroupCols = []
    if( indicspeciesGroupColsRaw instanceof List ) {
        indicspeciesGroupCols = indicspeciesGroupColsRaw.collect { it.toString().trim() }.findAll { it }
    } else if( indicspeciesGroupColsRaw ) {
        indicspeciesGroupCols = indicspeciesGroupColsRaw.toString().split(/[,|]/).collect { it.trim() }.findAll { it }
    } else {
        indicspeciesGroupCols = ['Depth', 'Month']
    }
    if( indicspeciesRequested && indicspeciesGroupCols.size() < 2 ) {
        exit 1, "indicspecies.group_cols must contain at least two groups when indicspecies.enabled is true"
    }
    boolean indicspeciesEnabled = indicspeciesRequested
    def indicspeciesSampleCol = indicspeciesConfig.sample_col ?: metadataPlotsSampleCol
    def indicspeciesPerms = indicspeciesConfig.perms ? (indicspeciesConfig.perms as int) : 9999
    def indicspeciesSeed = indicspeciesConfig.seed ? (indicspeciesConfig.seed as int) : 42
    def indicspeciesQThreshold = indicspeciesConfig.q_threshold != null ? (indicspeciesConfig.q_threshold as double) : 0.05d
    if( indicspeciesQThreshold < 0 || indicspeciesQThreshold > 1 ) {
        exit 1, "indicspecies.q_threshold must be between 0 and 1"
    }
    def indicspeciesMinN = indicspeciesConfig.min_n ? (indicspeciesConfig.min_n as int) : 2
    def indicspeciesBlockCol = indicspeciesConfig.block_col ? indicspeciesConfig.block_col.toString().trim() : ''
    def indicspeciesStratifiedConfig = indicspeciesConfig.stratified ?: [:]
    boolean indicspeciesStratifiedEnabled = indicspeciesStratifiedConfig instanceof Map ?
        (indicspeciesStratifiedConfig.containsKey('enabled') ? (indicspeciesStratifiedConfig.enabled as boolean) : false) :
        false
    List<String> indicspeciesStratifiedSpecs = []
    if( indicspeciesStratifiedEnabled ) {
        def stratifiedAnalysesRaw = indicspeciesStratifiedConfig.analyses ?: []
        if( !(stratifiedAnalysesRaw instanceof List) ) {
            exit 1, "indicspecies.stratified.analyses must be a list when indicspecies.stratified.enabled is true"
        }
        indicspeciesStratifiedSpecs = stratifiedAnalysesRaw.collect { analysis ->
            if( !(analysis instanceof Map) ) {
                exit 1, "Each indicspecies.stratified.analyses entry must be a map with within_col and group_col"
            }
            def withinCol = (analysis.within_col ?: analysis.within ?: '').toString().trim()
            def groupCol = (analysis.group_col ?: analysis.group ?: '').toString().trim()
            if( !withinCol || !groupCol ) {
                exit 1, "Each indicspecies.stratified.analyses entry requires within_col and group_col"
            }
            def unsafeValues = [withinCol, groupCol].findAll { it.contains('::') || it.contains(';') || it.contains('|') }
            if( unsafeValues ) {
                exit 1, "indicspecies.stratified column names cannot contain '::', ';', or '|': ${unsafeValues.join(', ')}"
            }
            def levelsRaw = analysis.levels ?: analysis.within_values ?: analysis.sample_types ?: []
            List<String> levels = []
            if( levelsRaw instanceof List ) {
                levels = levelsRaw.collect { it.toString().trim() }.findAll { it }
            } else if( levelsRaw ) {
                levels = levelsRaw.toString().split(/\|/).collect { it.trim() }.findAll { it }
            }
            def unsafeLevels = levels.findAll { it.contains('::') || it.contains(';') || it.contains('|') }
            if( unsafeLevels ) {
                exit 1, "indicspecies.stratified levels cannot contain '::', ';', or '|': ${unsafeLevels.join(', ')}"
            }
            levels ? "${withinCol}::${groupCol}::${levels.join('|')}" : "${withinCol}::${groupCol}"
        }.findAll { it }
        if( indicspeciesStratifiedSpecs.isEmpty() ) {
            exit 1, "indicspecies.stratified.enabled is true but no valid analyses were configured"
        }
    }
    def indicspeciesStratifiedSpecsArg = indicspeciesStratifiedSpecs.join(';')
    def indicspeciesGroup1 = indicspeciesGroupCols ? indicspeciesGroupCols[0] : 'group1'
    def indicspeciesGroup2 = indicspeciesGroupCols.size() > 1 ? indicspeciesGroupCols[1] : ''
    def indicspeciesOutputDirAbs = new File(outputDir, "indicspecies").canonicalPath
    boolean indicspeciesPlotEnabled = indicspeciesConfig.containsKey('plot_enabled') ? (indicspeciesConfig.plot_enabled as boolean) : true
    def indicspeciesPlotPairsMode = indicspeciesConfig.plot_pairs_mode ?: 'all'
    def indicspeciesPlotOutputDir = indicspeciesConfig.plot_output_dir ?: 'indicspecies/plots'
    def indicspeciesPlotOutputDirAbs = new File(outputDir, indicspeciesPlotOutputDir).canonicalPath
    def indicspeciesPlotVennPath = indicspeciesConfig.venn ? resolveOptionalPath(indicspeciesConfig.venn, configRoot) : null
    def indicspeciesPlotTaxonomyPath = indicspeciesConfig.taxonomy ? resolveOptionalPath(indicspeciesConfig.taxonomy, configRoot) : new File(outputDir, 'taxonomy/ASV_SILVA_tax.full-length.vsearch.tsv').canonicalPath
    def indicspeciesColorCol = indicspeciesConfig.color_col ?: metadataPlotsColorCol
    def indicspeciesGroupPaletteMap = extractNamedStringMap(indicspeciesConfig as Map, indicspeciesGroupCols, 'group_palettes', 'palette')
    def indicspeciesGroupOrderMap = extractNamedListMap(indicspeciesConfig as Map, indicspeciesGroupCols, 'group_orders', 'order')
    def indicspeciesFocusLabelMap = extractNamedStringMap(indicspeciesConfig as Map, indicspeciesGroupCols, 'focus_labels', 'focus_label')
    if( indicspeciesGroup1 && !indicspeciesGroupOrderMap.containsKey(indicspeciesGroup1) && metadataPlotsGroupOrder ) {
        indicspeciesGroupOrderMap[indicspeciesGroup1] = metadataPlotsGroupOrder
    }
    def indicspeciesGroup1Palette = indicspeciesGroupPaletteMap[indicspeciesGroup1] ?: ''
    def indicspeciesGroup2Palette = indicspeciesGroup2 ? (indicspeciesGroupPaletteMap[indicspeciesGroup2] ?: '') : ''
    List<String> indicspeciesGroup1Order = indicspeciesGroupOrderMap[indicspeciesGroup1] ?: []
    List<String> indicspeciesGroup2Order = indicspeciesGroup2 ? (indicspeciesGroupOrderMap[indicspeciesGroup2] ?: []) : []
    def indicspeciesFocusGroup1Label = indicspeciesFocusLabelMap[indicspeciesGroup1] ?: ''
    def indicspeciesFocusGroup2Label = indicspeciesGroup2 ? (indicspeciesFocusLabelMap[indicspeciesGroup2] ?: '') : ''
    def indicspeciesGroupColsCsv = indicspeciesGroupCols.join(',')
    def indicspeciesGroupPaletteJson = groovy.json.JsonOutput.toJson(indicspeciesGroupPaletteMap)
    def indicspeciesGroupOrderJson = groovy.json.JsonOutput.toJson(indicspeciesGroupOrderMap)
    def indicspeciesFocusLabelJson = groovy.json.JsonOutput.toJson(indicspeciesFocusLabelMap)
    boolean indicspeciesLabelFocusedAsvs = indicspeciesConfig.containsKey('label_focused_asvs') ? (indicspeciesConfig.label_focused_asvs as boolean) : false
    boolean indicspeciesAlignedEnabled = indicspeciesConfig.containsKey('aligned_plot_enabled') ? (indicspeciesConfig.aligned_plot_enabled as boolean) : false
    boolean indicspeciesUseDuleg = indicspeciesConfig.containsKey('use_duleg') ? (indicspeciesConfig.use_duleg as boolean) : false
    def indicspeciesAlignedOutputDir = indicspeciesConfig.aligned_plot_output_dir ?: 'indicspecies/aligned'
    def indicspeciesAlignedOutputDirAbs = new File(outputDir, indicspeciesAlignedOutputDir).canonicalPath
    def indicspeciesAlignedAlpha = indicspeciesConfig.aligned_alpha != null ? (indicspeciesConfig.aligned_alpha as double) : 0.05d
    def indicspeciesAlignedMinStat = indicspeciesConfig.aligned_min_stat != null ? (indicspeciesConfig.aligned_min_stat as double) : 0.0d
    def indicspeciesAlignedTopN = indicspeciesConfig.aligned_top_n ? (indicspeciesConfig.aligned_top_n as int) : 25

    def vocCorrelationConfig = config.voc_correlation ?: [:]
    boolean vocCorrelationRequested = vocCorrelationConfig.containsKey('enabled') ? (vocCorrelationConfig.enabled as boolean) : false
    if( vocCorrelationRequested && !metadataPlotsEnabled ) {
        exit 1, "voc_correlation.enabled requires metadata_plots.enabled to be true"
    }
    def vocCorrelationVocTablePath = vocCorrelationConfig.voc_table ? resolveOptionalPath(vocCorrelationConfig.voc_table, configRoot) : null
    if( vocCorrelationRequested && (!vocCorrelationVocTablePath || !new File(vocCorrelationVocTablePath).exists()) ) {
        exit 1, "voc_correlation.voc_table must point to an existing VOC table when voc_correlation.enabled is true"
    }
    boolean vocCorrelationEnabled = vocCorrelationRequested
    def vocCorrelationOutputDir = vocCorrelationConfig.output_dir ?: 'voc_correlation'
    def vocCorrelationOutputDirAbs = resolveOutputRelative(vocCorrelationOutputDir.toString(), outputDir)
    def vocCorrelationVocSampleCol = vocCorrelationConfig.voc_sample_col ? vocCorrelationConfig.voc_sample_col.toString().trim() : 'sample'
    def vocCorrelationSampleIdMode = vocCorrelationConfig.sample_id_mode ? vocCorrelationConfig.sample_id_mode.toString().trim() : 'legacy_patient_pair'
    def vocCorrelationMetadataSampleCol = vocCorrelationConfig.metadata_sample_col ? vocCorrelationConfig.metadata_sample_col.toString().trim() : metadataPlotsSampleCol
    def vocCorrelationTypeCol = vocCorrelationConfig.type_col ? vocCorrelationConfig.type_col.toString().trim() : metadataPlotsTypeCol
    def vocCorrelationPatientCol = vocCorrelationConfig.patient_col ? vocCorrelationConfig.patient_col.toString().trim() : 'Participant_ID'
    def vocCorrelationCaseCol = vocCorrelationConfig.case_col ? vocCorrelationConfig.case_col.toString().trim() : 'Case'
    def vocCorrelationSampleTypesRaw = vocCorrelationConfig.sample_types ?: 'Bronchial Brush,Lung Brush'
    def vocCorrelationSampleTypes = vocCorrelationSampleTypesRaw instanceof List ?
        vocCorrelationSampleTypesRaw.collect { it.toString().trim() }.findAll { it }.join(',') :
        vocCorrelationSampleTypesRaw.toString().trim()
    boolean vocCorrelationUseLegacySubset = vocCorrelationConfig.containsKey('use_legacy_voc_subset') ? (vocCorrelationConfig.use_legacy_voc_subset as boolean) : true
    def vocCorrelationVocColsRaw = vocCorrelationConfig.voc_columns
    List<String> vocCorrelationVocCols = []
    if( vocCorrelationVocColsRaw instanceof List ) {
        vocCorrelationVocCols = vocCorrelationVocColsRaw.collect { it.toString() }.findAll { it?.trim() }
    } else if( vocCorrelationVocColsRaw ) {
        vocCorrelationVocCols = vocCorrelationVocColsRaw.toString().split(/\r?\n|\|/).collect { it.trim() }.findAll { it }
    }
    def vocCorrelationDirection = vocCorrelationConfig.correlation_direction ? vocCorrelationConfig.correlation_direction.toString().trim().toLowerCase() : 'positive'
    if( !(vocCorrelationDirection in ['positive','negative','both']) ) {
        exit 1, "voc_correlation.correlation_direction must be one of: positive, negative, both"
    }
    def vocCorrelationCasePalette = vocCorrelationConfig.case_palette ?: (indicspeciesGroupPaletteMap[vocCorrelationCaseCol] ?: '')
    def vocCorrelationIsaPalette = vocCorrelationConfig.isa_palette ?: (indicspeciesGroupPaletteMap[vocCorrelationTypeCol] ?: '')

    def measurementAssociationConfig = config.measurement_association ?: [:]
    boolean measurementAssociationRequested = measurementAssociationConfig.containsKey('enabled') ? (measurementAssociationConfig.enabled as boolean) : false
    if( measurementAssociationRequested && !metadataPlotsEnabled ) {
        exit 1, "measurement_association.enabled requires metadata_plots.enabled to be true"
    }
    boolean measurementAssociationEnabled = measurementAssociationRequested
    def measurementAssociationOutputDir = measurementAssociationConfig.output_dir ?: 'measurement_association'
    def measurementAssociationOutputDirAbs = resolveOutputRelative(measurementAssociationOutputDir.toString(), outputDir)
    def measurementAssociationTablePath = measurementAssociationConfig.measurement_table ? resolveOptionalPath(measurementAssociationConfig.measurement_table, configRoot) : null
    if( measurementAssociationEnabled && measurementAssociationTablePath && !new File(measurementAssociationTablePath).exists() ) {
        exit 1, "measurement_association.measurement_table was configured but does not exist: ${measurementAssociationTablePath}"
    }
    def measurementAssociationSampleCol = measurementAssociationConfig.sample_col ? measurementAssociationConfig.sample_col.toString().trim() : metadataPlotsSampleCol
    def measurementAssociationAsvIdCol = measurementAssociationConfig.asv_id_col ? measurementAssociationConfig.asv_id_col.toString().trim() : 'ASV_ID'
    def measurementAssociationMeasurementSampleCol = measurementAssociationConfig.measurement_sample_col ? measurementAssociationConfig.measurement_sample_col.toString().trim() : measurementAssociationSampleCol
    def measurementAssociationMetadataJoinRaw = measurementAssociationConfig.metadata_join_cols ?: ''
    List<String> measurementAssociationMetadataJoinCols = []
    if( measurementAssociationMetadataJoinRaw instanceof List ) {
        measurementAssociationMetadataJoinCols = measurementAssociationMetadataJoinRaw.collect { it.toString().trim() }.findAll { it }
    } else if( measurementAssociationMetadataJoinRaw ) {
        measurementAssociationMetadataJoinCols = measurementAssociationMetadataJoinRaw.toString().split(/[,|]/).collect { it.trim() }.findAll { it }
    }
    def measurementAssociationMeasurementJoinRaw = measurementAssociationConfig.measurement_join_cols ?: ''
    List<String> measurementAssociationMeasurementJoinCols = []
    if( measurementAssociationMeasurementJoinRaw instanceof List ) {
        measurementAssociationMeasurementJoinCols = measurementAssociationMeasurementJoinRaw.collect { it.toString().trim() }.findAll { it }
    } else if( measurementAssociationMeasurementJoinRaw ) {
        measurementAssociationMeasurementJoinCols = measurementAssociationMeasurementJoinRaw.toString().split(/[,|]/).collect { it.trim() }.findAll { it }
    }
    def measurementAssociationColsRaw = measurementAssociationConfig.measurement_cols ?: []
    List<String> measurementAssociationCols = []
    if( measurementAssociationColsRaw instanceof List ) {
        measurementAssociationCols = measurementAssociationColsRaw.collect { it.toString().trim() }.findAll { it }
    } else if( measurementAssociationColsRaw ) {
        measurementAssociationCols = measurementAssociationColsRaw.toString().split(/\r?\n|\|/).collect { it.trim() }.findAll { it }
    }
    def measurementAssociationExcludeRaw = measurementAssociationConfig.exclude_cols ?: []
    List<String> measurementAssociationExcludeCols = []
    if( measurementAssociationExcludeRaw instanceof List ) {
        measurementAssociationExcludeCols = measurementAssociationExcludeRaw.collect { it.toString().trim() }.findAll { it }
    } else if( measurementAssociationExcludeRaw ) {
        measurementAssociationExcludeCols = measurementAssociationExcludeRaw.toString().split(/\r?\n|\|/).collect { it.trim() }.findAll { it }
    }
    def measurementAssociationGroupCol = measurementAssociationConfig.group_col ? measurementAssociationConfig.group_col.toString().trim() : metadataPlotsTypeCol
    def measurementAssociationGroupPalette = measurementAssociationConfig.group_palette ? measurementAssociationConfig.group_palette.toString().trim() : ''
    def measurementAssociationMaxAsvs = measurementAssociationConfig.max_asvs ? (measurementAssociationConfig.max_asvs as int) : 300
    def measurementAssociationMinTotal = measurementAssociationConfig.min_total != null ? (measurementAssociationConfig.min_total as double) : 0.0d
    def measurementAssociationMinPrevalence = measurementAssociationConfig.min_prevalence != null ? (measurementAssociationConfig.min_prevalence as double) : 0.0d
    def measurementAssociationTopCorrelations = measurementAssociationConfig.top_correlations ? (measurementAssociationConfig.top_correlations as int) : 100
    def measurementAssociationDirection = measurementAssociationConfig.correlation_direction ? measurementAssociationConfig.correlation_direction.toString().trim().toLowerCase() : 'both'
    if( !(measurementAssociationDirection in ['positive','negative','both']) ) {
        exit 1, "measurement_association.correlation_direction must be one of: positive, negative, both"
    }
    def measurementAssociationMethodsRaw = measurementAssociationConfig.ordination_methods ?: 'cca,rda,dbrda'
    def measurementAssociationMethods = measurementAssociationMethodsRaw instanceof List ?
        measurementAssociationMethodsRaw.collect { it.toString().trim().toLowerCase() }.findAll { it }.join(',') :
        measurementAssociationMethodsRaw.toString().trim().toLowerCase()
    def measurementAssociationPermutations = measurementAssociationConfig.permutations ? (measurementAssociationConfig.permutations as int) : 999
    def measurementAssociationTopVectors = measurementAssociationConfig.top_vectors ? (measurementAssociationConfig.top_vectors as int) : 12
    def measurementAssociationFormatsRaw = measurementAssociationConfig.formats ?: 'pdf,png,svg'
    def measurementAssociationFormats = measurementAssociationFormatsRaw instanceof List ?
        measurementAssociationFormatsRaw.collect { it.toString().trim() }.findAll { it }.join(',') :
        measurementAssociationFormatsRaw.toString().trim()

    def groupingDiagnosticsConfig = config.grouping_diagnostics ?: [:]
    boolean groupingDiagnosticsRequested = groupingDiagnosticsConfig.containsKey('enabled') ? (groupingDiagnosticsConfig.enabled as boolean) : false
    if( groupingDiagnosticsRequested && !metadataPlotsEnabled ) {
        exit 1, "grouping_diagnostics.enabled requires metadata_plots.enabled to be true"
    }
    boolean groupingDiagnosticsEnabled = groupingDiagnosticsRequested
    def groupingDiagnosticsOutputDir = groupingDiagnosticsConfig.output_dir ?: 'grouping_diagnostics'
    def groupingDiagnosticsOutputDirAbs = resolveOutputRelative(groupingDiagnosticsOutputDir.toString(), outputDir)
    def groupingDiagnosticsSampleCol = groupingDiagnosticsConfig.sample_col ? groupingDiagnosticsConfig.sample_col.toString().trim() : metadataPlotsSampleCol
    def groupingDiagnosticsGroupColsRaw = groupingDiagnosticsConfig.group_cols ?: [metadataPlotsTypeCol]
    List<String> groupingDiagnosticsGroupCols = []
    if( groupingDiagnosticsGroupColsRaw instanceof List ) {
        groupingDiagnosticsGroupCols = groupingDiagnosticsGroupColsRaw.collect { it.toString().trim() }.findAll { it }
    } else if( groupingDiagnosticsGroupColsRaw ) {
        groupingDiagnosticsGroupCols = groupingDiagnosticsGroupColsRaw.toString().split(/[,|]/).collect { it.trim() }.findAll { it }
    }
    if( groupingDiagnosticsRequested && groupingDiagnosticsGroupCols.isEmpty() ) {
        exit 1, "grouping_diagnostics.group_cols must contain at least one metadata column when enabled"
    }
    def groupingDiagnosticsBaselineGroup = groupingDiagnosticsConfig.baseline_group ? groupingDiagnosticsConfig.baseline_group.toString().trim() : ''
    def groupingDiagnosticsPrimaryGroup = groupingDiagnosticsConfig.primary_group ? groupingDiagnosticsConfig.primary_group.toString().trim() : ''
    def groupingDiagnosticsPaletteMap = extractNamedStringMap(groupingDiagnosticsConfig as Map, groupingDiagnosticsGroupCols, 'group_palettes', 'palette')
    def groupingDiagnosticsOrderMap = extractNamedListMap(groupingDiagnosticsConfig as Map, groupingDiagnosticsGroupCols, 'group_orders', 'order')
    def groupingDiagnosticsSharedPaletteConfig = config.indicspecies?.group_palettes instanceof Map ? config.indicspecies.group_palettes : [:]
    groupingDiagnosticsGroupCols.each { col ->
        if( !groupingDiagnosticsPaletteMap.containsKey(col) && groupingDiagnosticsSharedPaletteConfig[col] ) {
            groupingDiagnosticsPaletteMap[col] = groupingDiagnosticsSharedPaletteConfig[col]
        }
    }
    if( metadataPlotsTypeCol && metadataPlotsGroupOrder && groupingDiagnosticsGroupCols.contains(metadataPlotsTypeCol) && !groupingDiagnosticsOrderMap.containsKey(metadataPlotsTypeCol) ) {
        groupingDiagnosticsOrderMap[metadataPlotsTypeCol] = metadataPlotsGroupOrder
    }
    def groupingDiagnosticsPaletteJson = groovy.json.JsonOutput.toJson(groupingDiagnosticsPaletteMap)
    def groupingDiagnosticsOrderJson = groovy.json.JsonOutput.toJson(groupingDiagnosticsOrderMap)
    def groupingDiagnosticsMetricsRaw = groupingDiagnosticsConfig.distance_metrics ?: 'bray'
    def groupingDiagnosticsMetrics = groupingDiagnosticsMetricsRaw instanceof List ?
        groupingDiagnosticsMetricsRaw.collect { it.toString().trim().toLowerCase() }.findAll { it }.join(',') :
        groupingDiagnosticsMetricsRaw.toString().trim().toLowerCase()
    def groupingDiagnosticsTransform = groupingDiagnosticsConfig.transform ? groupingDiagnosticsConfig.transform.toString().trim().toLowerCase() : 'relative'
    def groupingDiagnosticsPermutations = groupingDiagnosticsConfig.permutations ? (groupingDiagnosticsConfig.permutations as int) : 999
    def groupingDiagnosticsRandomState = groupingDiagnosticsConfig.random_state ? (groupingDiagnosticsConfig.random_state as int) : 42
    def groupingDiagnosticsFormatsRaw = groupingDiagnosticsConfig.formats ?: 'pdf,png,svg'
    def groupingDiagnosticsFormats = groupingDiagnosticsFormatsRaw instanceof List ?
        groupingDiagnosticsFormatsRaw.collect { it.toString().trim() }.findAll { it }.join(',') :
        groupingDiagnosticsFormatsRaw.toString().trim()
    def groupingDiagnosticsSoftLabelConfig = groupingDiagnosticsConfig.soft_labeling instanceof Map ? groupingDiagnosticsConfig.soft_labeling : [:]
    boolean groupingDiagnosticsSoftLabelEnabled = groupingDiagnosticsSoftLabelConfig.containsKey('enabled') ? (groupingDiagnosticsSoftLabelConfig.enabled as boolean) : false
    def groupingDiagnosticsSoftLabelK = groupingDiagnosticsSoftLabelConfig.k ? (groupingDiagnosticsSoftLabelConfig.k as int) : 7
    def groupingDiagnosticsSoftLabelTargetColsRaw = groupingDiagnosticsSoftLabelConfig.target_cols ?: (groupingDiagnosticsPrimaryGroup ? [groupingDiagnosticsPrimaryGroup] : [])
    List<String> groupingDiagnosticsSoftLabelTargetCols = groupingDiagnosticsSoftLabelTargetColsRaw instanceof List ?
        groupingDiagnosticsSoftLabelTargetColsRaw.collect { it.toString().trim() }.findAll { it } :
        groupingDiagnosticsSoftLabelTargetColsRaw.toString().split(/[,|]/).collect { it.trim() }.findAll { it }
    def groupingDiagnosticsSoftLabelExcludeRaw = groupingDiagnosticsSoftLabelConfig.exclude_labels ?: ['outlier']
    List<String> groupingDiagnosticsSoftLabelExcludeLabels = groupingDiagnosticsSoftLabelExcludeRaw instanceof List ?
        groupingDiagnosticsSoftLabelExcludeRaw.collect { it.toString().trim() }.findAll { it } :
        groupingDiagnosticsSoftLabelExcludeRaw.toString().split(/[,|]/).collect { it.trim() }.findAll { it }
    def groupingDiagnosticsSoftLabelMinClassSamples = groupingDiagnosticsSoftLabelConfig.min_class_samples ? (groupingDiagnosticsSoftLabelConfig.min_class_samples as int) : 3
    def groupingDiagnosticsSoftLabelDistanceQuantile = groupingDiagnosticsSoftLabelConfig.distance_quantile != null ? (groupingDiagnosticsSoftLabelConfig.distance_quantile as double) : 0.95d
    boolean groupingDiagnosticsApplySoftLabels = groupingDiagnosticsSoftLabelConfig.containsKey('apply_downstream') ? (groupingDiagnosticsSoftLabelConfig.apply_downstream as boolean) : false
    def groupingDiagnosticsSoftLabelTargetCol = groupingDiagnosticsSoftLabelConfig.target_col ? groupingDiagnosticsSoftLabelConfig.target_col.toString().trim() : (groupingDiagnosticsSoftLabelTargetCols ? groupingDiagnosticsSoftLabelTargetCols[0] : '')
    def groupingDiagnosticsSoftLabelMinConfidence = groupingDiagnosticsSoftLabelConfig.min_confidence != null ? (groupingDiagnosticsSoftLabelConfig.min_confidence as double) : 0.70d
    def groupingDiagnosticsSoftLabelMinNeighborAgreement = groupingDiagnosticsSoftLabelConfig.min_neighbor_agreement != null ? (groupingDiagnosticsSoftLabelConfig.min_neighbor_agreement as double) : 0.60d
    def groupingDiagnosticsSoftLabelMinCvBalancedAccuracy = groupingDiagnosticsSoftLabelConfig.min_cv_balanced_accuracy != null ? (groupingDiagnosticsSoftLabelConfig.min_cv_balanced_accuracy as double) : 0.60d
    if( groupingDiagnosticsApplySoftLabels && (!groupingDiagnosticsEnabled || !groupingDiagnosticsSoftLabelEnabled || !groupingDiagnosticsSoftLabelTargetCol) ) {
        exit 1, "grouping_diagnostics.soft_labeling.apply_downstream requires enabled diagnostics, enabled soft labeling, and a target_col"
    }
    def groupingDiagnosticsPowerConfig = groupingDiagnosticsConfig.power instanceof Map ? groupingDiagnosticsConfig.power : [:]
    boolean groupingDiagnosticsPowerEnabled = groupingDiagnosticsPowerConfig.containsKey('enabled') ? (groupingDiagnosticsPowerConfig.enabled as boolean) : false
    def groupingDiagnosticsPowerSizesRaw = groupingDiagnosticsPowerConfig.sample_sizes ?: '3,5,10,15,20'
    def groupingDiagnosticsPowerSizes = groupingDiagnosticsPowerSizesRaw instanceof List ?
        groupingDiagnosticsPowerSizesRaw.collect { it.toString().trim() }.findAll { it }.join(',') :
        groupingDiagnosticsPowerSizesRaw.toString().trim()
    def groupingDiagnosticsPowerSimulations = groupingDiagnosticsPowerConfig.simulations ? (groupingDiagnosticsPowerConfig.simulations as int) : 100
    def groupingDiagnosticsPowerPermutations = groupingDiagnosticsPowerConfig.permutations ? (groupingDiagnosticsPowerConfig.permutations as int) : 99
    def groupingDiagnosticsPowerAlpha = groupingDiagnosticsPowerConfig.alpha != null ? (groupingDiagnosticsPowerConfig.alpha as double) : 0.05d
    def groupingDiagnosticsPowerMinGroups = groupingDiagnosticsPowerConfig.min_groups ? (groupingDiagnosticsPowerConfig.min_groups as int) : 2

    def clustermapsConfig = config.clustermaps ?: [:]
    boolean clustermapsRequested = clustermapsConfig.containsKey('enabled') ? (clustermapsConfig.enabled as boolean) : false
    if( clustermapsRequested && !metadataPlotsEnabled ) {
        exit 1, "clustermaps.enabled requires metadata_plots.enabled to be true"
    }
    boolean clustermapsEnabled = clustermapsRequested
    def clustermapsOutputDir = clustermapsConfig.output_dir ?: 'clustermaps'
    def clustermapsOutputDirAbs = new File(outputDir, clustermapsOutputDir).canonicalPath
    def clustermapsMitoOutputDir = clustermapsConfig.mito_output_dir ?: 'mito/clustermaps'
    def clustermapsMitoOutputDirAbs = new File(outputDir, clustermapsMitoOutputDir).canonicalPath
    def clustermapsMitoInputPath = clustermapsConfig.mito_input ? resolveOptionalPath(clustermapsConfig.mito_input, configRoot) : new File(outputDir, 'mito/ASVs/ASV_target.mito.tsv').canonicalPath
    def clustermapsIsaFile = clustermapsConfig.isa_file ? resolveOptionalPath(clustermapsConfig.isa_file, configRoot) : null
    def clustermapsIsaSearchDir = (clustermapsIsaFile && new File(clustermapsIsaFile).isDirectory()) ? clustermapsIsaFile : null
    def clustermapsSampleCol = clustermapsConfig.sample_col ?: 'sample'
    def clustermapsSampleCodeCol = clustermapsConfig.sample_code_col ?: 'sample_code'
    def clustermapsAsvIdCol = clustermapsConfig.asv_id_col ?: 'ASV_ID'
    def clustermapsGroup1Col = clustermapsConfig.group1_col ?: 'type_group'
    def clustermapsGroup2Col = clustermapsConfig.group2_col ?: 'status'
    def clustermapsGroup3Col = clustermapsConfig.containsKey('group3_col') ? (clustermapsConfig.group3_col ?: '') : 'kit'
    def clustermapsGroup1OrderRaw = clustermapsConfig.group1_order ?: (clustermapsConfig.type_order ?: '')
    List<String> clustermapsGroup1Order = normalizePresetList(clustermapsGroup1OrderRaw, [], config.order_presets ?: [:])
    def clustermapsExcludeGroup1 = clustermapsConfig.exclude_group1 ?: (clustermapsConfig.exclude_types ?: '')
    def clustermapsGroup1Palette = clustermapsConfig.group1_palette ?: (clustermapsConfig.type_palette ?: '')
    def clustermapsGroup2Palette = clustermapsConfig.group2_palette ?: (clustermapsConfig.status_palette ?: '')
    def clustermapsGroup3Palette = clustermapsConfig.group3_palette ?: (clustermapsConfig.kit_palette ?: '')
    def clustermapsRanks = clustermapsConfig.ranks ?: 'Phylum,Class,Order,Family,Genus,Species,ASV_ID'
    def clustermapsTopN = clustermapsConfig.topN ?: 'Phylum=30,Class=30,Order=30,Family=30,Genus=30,Species=30,ASV_ID=6000'
    def clustermapsCountCol = clustermapsConfig.count_col ?: 'corr_count'
    def clustermapsIsaMinStat = clustermapsConfig.isa_min_stat != null ? (clustermapsConfig.isa_min_stat as double) : 0.6d
    def clustermapsIsaSignificanceCols = clustermapsConfig.isa_significance_cols ?: ''
    def clustermapsIsaStatCols = clustermapsConfig.isa_stat_cols ?: ''
    def clustermapsFormats = clustermapsConfig.formats ?: 'pdf,png,svg'
    def clustermapsFigWidth = clustermapsConfig.figwidth != null ? clustermapsConfig.figwidth : null
    def clustermapsRowHeight = clustermapsConfig.row_height != null ? clustermapsConfig.row_height : null
    def clustermapsMinHeight = clustermapsConfig.min_height != null ? clustermapsConfig.min_height : null
    def clustermapsMaxHeight = clustermapsConfig.max_height != null ? clustermapsConfig.max_height : null
    def clustermapsMitoSampleMode = clustermapsConfig.mito_sample_mode ?: 'auto'
    boolean clustermapsRunMito = clustermapsConfig.containsKey('run_mito') ? (clustermapsConfig.run_mito as boolean) : true
    def clustermapsIsaAutoCandidates = ([clustermapsGroup3Col, clustermapsGroup1Col, clustermapsGroup2Col] + indicspeciesGroupCols)
        .findAll { it }
        .collect { "${it}_indicator_species_summary.tsv" }
        .unique()
    def clustermapsIsaPathExists = clustermapsIsaFile ? new File(clustermapsIsaFile).exists() : false
    def clustermapsIsaMayBeProducedInRun = clustermapsIsaFile && indicspeciesEnabled && clustermapsIsaFile == indicspeciesOutputDirAbs
    if( clustermapsIsaFile && !clustermapsIsaPathExists ) {
        if( clustermapsIsaMayBeProducedInRun ) {
            log.info "clustermaps.isa_file points to the INDICSPECIES output directory and will be resolved at runtime: ${clustermapsIsaFile}"
        } else {
            log.warn "clustermaps.isa_file path not found; ISA-gated clustermaps will be skipped unless a matching table is produced at runtime: ${clustermapsIsaFile}"
        }
    }

    def spieceasiConfig = config.spieceasi ?: [:]
    boolean spieceasiRequested = spieceasiConfig.containsKey('enabled') ? (spieceasiConfig.enabled as boolean) : false
    if( spieceasiRequested && !metadataPlotsEnabled ) {
        exit 1, "spieceasi.enabled requires metadata_plots.enabled to be true"
    }
    boolean spieceasiEnabled = spieceasiRequested
    def spieceasiOutputDir = spieceasiConfig.output_dir ?: 'spieceasi'
    def spieceasiOutputDirAbs = new File(outputDir, spieceasiOutputDir).canonicalPath
    def spieceasiPrefix = spieceasiConfig.prefix ?: 'spieceasi'
    boolean spieceasiTranspose = spieceasiConfig.containsKey('transpose') ? (spieceasiConfig.transpose as boolean) : true
    def spieceasiMinRelAbund = spieceasiConfig.min_rel_abund != null ? (spieceasiConfig.min_rel_abund as double) : 0d
    def spieceasiMinPrevalence = spieceasiConfig.min_prevalence != null ? (spieceasiConfig.min_prevalence as double) : 0.25d
    boolean spieceasiRemoveZeroVar = spieceasiConfig.containsKey('remove_zero_var') ? (spieceasiConfig.remove_zero_var as boolean) : true
    def spieceasiMethod = spieceasiConfig.method ?: 'glasso'
    def spieceasiLambdaMinRatio = spieceasiConfig.lambda_min_ratio != null ? (spieceasiConfig.lambda_min_ratio as double) : 0.1d
    def spieceasiNlambda = spieceasiConfig.nlambda ? (spieceasiConfig.nlambda as int) : 20
    def spieceasiRepNum = spieceasiConfig.rep_num ? (spieceasiConfig.rep_num as int) : 50
    def spieceasiThresh = spieceasiConfig.thresh != null ? (spieceasiConfig.thresh as double) : 0.1d
    def spieceasiPulsarCriterion = spieceasiConfig.pulsar_criterion ? spieceasiConfig.pulsar_criterion.toString().trim().toLowerCase() : 'bstars'
    if( !['stars', 'bstars'].contains(spieceasiPulsarCriterion) ) {
        exit 1, "spieceasi.pulsar_criterion must be one of: stars, bstars"
    }
    def spieceasiNcores = spieceasiConfig.ncores ? (spieceasiConfig.ncores as int) : pipelineThreads
    def spieceasiSeed = spieceasiConfig.seed ? (spieceasiConfig.seed as int) : 10010
    def spieceasiEdgeThreshold = spieceasiConfig.edge_threshold != null ? (spieceasiConfig.edge_threshold as double) : 0.1d
    boolean spieceasiKeepNegative = spieceasiConfig.containsKey('keep_negative') ? (spieceasiConfig.keep_negative as boolean) : true
    boolean spieceasiAllPosOnly = spieceasiConfig.containsKey('all_pos_only') ? (spieceasiConfig.all_pos_only as boolean) : false
    def spieceasiLayoutIters = spieceasiConfig.layout_iters ? (spieceasiConfig.layout_iters as int) : 1000
    boolean spieceasiForceFilter = spieceasiConfig.containsKey('force_filter') ? (spieceasiConfig.force_filter as boolean) : false
    boolean spieceasiForceSpieceasi = spieceasiConfig.containsKey('force_spieceasi') ? (spieceasiConfig.force_spieceasi as boolean) : false
    boolean spieceasiForceGraphs = spieceasiConfig.containsKey('force_graphs') ? (spieceasiConfig.force_graphs as boolean) : true
    boolean networkRequested = spieceasiConfig.containsKey('network_enabled') ? (spieceasiConfig.network_enabled as boolean) : false
    if( networkRequested && !indicspeciesEnabled ) {
        exit 1, "spieceasi.network_enabled requires indicspecies.enabled to be true"
    }
    boolean networkEnabled = networkRequested && indicspeciesEnabled
    def networkGraphAllPath = spieceasiConfig.graph_pos_all ? resolveOptionalPath(spieceasiConfig.graph_pos_all, configRoot) : new File(spieceasiOutputDirAbs, "${spieceasiPrefix}_network_pos_all.graphml").canonicalPath
    def networkGraphThrPath = spieceasiConfig.graph_pos_sub ? resolveOptionalPath(spieceasiConfig.graph_pos_sub, configRoot) : new File(spieceasiOutputDirAbs, "${spieceasiPrefix}_network_pos_thr.graphml").canonicalPath
    def networkNodeFeaturesPath = spieceasiConfig.node_features ? resolveOptionalPath(spieceasiConfig.node_features, configRoot) : new File(spieceasiOutputDirAbs, "${spieceasiPrefix}_node_features.csv").canonicalPath
    if( networkEnabled && !spieceasiEnabled ) {
        [networkGraphAllPath, networkGraphThrPath, networkNodeFeaturesPath].each { p ->
            if( !new File(p).exists() ) {
                exit 1, "spieceasi.network_enabled is true while spieceasi.enabled is false, but required cached file is missing: ${p}"
            }
        }
    }
    def networkModesRaw = spieceasiConfig.network_modes
    List<String> networkModes = []
    if( networkModesRaw instanceof List ) {
        networkModes = networkModesRaw.collect { it.toString().trim() }.findAll { it }
    } else if( networkModesRaw ) {
        networkModes = networkModesRaw.toString().split(/[,|]/).collect { it.trim() }.findAll { it }
    }
    def networkIsaOverlayGroupsRaw = spieceasiConfig.isa_overlay_groups
    List<String> networkIsaOverlayGroups = []
    if( networkIsaOverlayGroupsRaw instanceof List ) {
        networkIsaOverlayGroups = networkIsaOverlayGroupsRaw.collect { it.toString().trim() }.findAll { it }
    } else if( networkIsaOverlayGroupsRaw ) {
        networkIsaOverlayGroups = networkIsaOverlayGroupsRaw.toString().split(/[,|]/).collect { it.trim() }.findAll { it }
    }
    if( networkIsaOverlayGroups.isEmpty() ) {
        networkIsaOverlayGroups = indicspeciesGroupCols
    }
    networkIsaOverlayGroups = networkIsaOverlayGroups.findAll { indicspeciesGroupCols.contains(it) }
    if( networkIsaOverlayGroups.isEmpty() ) {
        networkIsaOverlayGroups = indicspeciesGroupCols
    }
    def allPosOnlyNetworkModes = [
        'degree_all',
        'abundance_all',
        'group1_isa_all',
        'group1_isa_all_labeled',
        'group1_isa_mag_all',
        'group1_isa_mag_all_labeled',
        'group2_isa_all',
        'group2_isa_all_labeled',
        'group2_isa_mag_all',
        'group2_isa_mag_all_labeled',
        'module_all',
        'module_all_labeled',
        'mag_pair_all',
        'mag_pair_all_labeled',
        'mag_pair_tax_all',
        'mag_pair_tax_all_labeled',
        'phylum_abund_all',
        'phylum_isa_all',
        'phylum_isa_all_labeled'
    ]
    if( networkIsaOverlayGroups.size() > 2 ) {
        networkIsaOverlayGroups.drop(2).eachWithIndex { groupName, offset ->
            def idx = offset + 3
            allPosOnlyNetworkModes.addAll([
                "group${idx}_isa_all",
                "group${idx}_isa_all_labeled",
                "group${idx}_isa_mag_all"
            ])
        }
    }
    if( networkModes.isEmpty() ) {
        networkModes = spieceasiAllPosOnly ? allPosOnlyNetworkModes : ['all']
    }
    if( spieceasiAllPosOnly ) {
        def modeRemap = [
            'all': null,
            'degree_sub': 'degree_all',
            'abundance_sub': 'abundance_all',
            'group1_isa': 'group1_isa_all',
            'group1_isa_labeled': 'group1_isa_all_labeled',
            'group1_isa_mag': 'group1_isa_mag_all',
            'group1_isa_mag_labeled': 'group1_isa_mag_all_labeled',
            'group1_isa_focus': 'group1_isa_focus_all',
            'group1_isa_focus_labeled': 'group1_isa_focus_all_labeled',
            'group2_isa': 'group2_isa_all',
            'group2_isa_labeled': 'group2_isa_all_labeled',
            'group2_isa_mag': 'group2_isa_mag_all',
            'group2_isa_mag_labeled': 'group2_isa_mag_all_labeled',
            'module_sub': 'module_all',
            'module_sub_labeled': 'module_all_labeled',
            'mag_pair_sub': 'mag_pair_all',
            'mag_pair_sub_labeled': 'mag_pair_all_labeled',
            'mag_pair_tax_sub': 'mag_pair_tax_all',
            'mag_pair_tax_sub_labeled': 'mag_pair_tax_all_labeled',
            'phylum_abund': 'phylum_abund_all',
            'phylum_isa': 'phylum_isa_all',
            'phylum_isa_labeled': 'phylum_isa_all_labeled'
        ]
        networkModes = networkModes.collect { mode ->
            def remapped = modeRemap.containsKey(mode) ? modeRemap[mode] : mode
            if( remapped == null ) {
                return null
            }
            def groupMode = (remapped =~ /^(group\d+_isa(?:_mag|_focus)?)(?:_labeled)?$/)
            if( groupMode.matches() ) {
                return remapped.contains('_labeled') ? "${groupMode[0][1]}_all_labeled" : "${groupMode[0][1]}_all"
            }
            return remapped
        }
            .findAll { it }
            .unique()
        if( networkModes.isEmpty() ) {
            networkModes = allPosOnlyNetworkModes
        }
    }
    def networkLayoutSeed = spieceasiConfig.layout_seed ? (spieceasiConfig.layout_seed as int) : 42
    def networkLayoutScale = spieceasiConfig.layout_scale != null ? (spieceasiConfig.layout_scale as double) : 3.0d
    def networkDegreeScale = spieceasiConfig.degree_scale != null ? (spieceasiConfig.degree_scale as double) : 80.0d
    def networkDegreeSizeMode = spieceasiConfig.degree_size_mode ? spieceasiConfig.degree_size_mode.toString().trim() : 'legacy'
    def networkDegreeMinArea = spieceasiConfig.degree_min_area != null ? (spieceasiConfig.degree_min_area as double) : 0.0d
    def networkEdgeWidthScale = spieceasiConfig.edge_width_scale != null ? (spieceasiConfig.edge_width_scale as double) : 5.0d
    def networkIsaScale = spieceasiConfig.isa_scale != null ? (spieceasiConfig.isa_scale as double) : 700.0d
    def networkAbundanceSizeMode = spieceasiConfig.abundance_size_mode ? spieceasiConfig.abundance_size_mode.toString().trim() : 'legacy'
    def networkAbundanceReference = spieceasiConfig.abundance_reference != null ? (spieceasiConfig.abundance_reference as double) : 5000.0d
    def networkAbundanceReferenceArea = spieceasiConfig.abundance_reference_area != null ? (spieceasiConfig.abundance_reference_area as double) : 80.0d
    def networkAbundanceMinArea = spieceasiConfig.abundance_min_area != null ? (spieceasiConfig.abundance_min_area as double) : 8.0d
    def networkAbundanceMaxArea = spieceasiConfig.abundance_max_area != null ? (spieceasiConfig.abundance_max_area as double) : 420.0d
    def networkAbundanceScalePower = spieceasiConfig.abundance_scale_power != null ? (spieceasiConfig.abundance_scale_power as double) : 1.6d
    boolean networkModuleBestOnly = spieceasiConfig.containsKey('module_best_only') ? (spieceasiConfig.module_best_only as boolean) : true
    def networkModuleBestMinSize = spieceasiConfig.module_best_min_size ? (spieceasiConfig.module_best_min_size as int) : 5
    def networkModuleBestMinStability = spieceasiConfig.module_best_min_stability != null ? (spieceasiConfig.module_best_min_stability as double) : 0.7d
    boolean networkModuleIsaOnly = spieceasiConfig.containsKey('module_isa_only') ? (spieceasiConfig.module_isa_only as boolean) : false
    boolean networkModuleColorByIsa = spieceasiConfig.containsKey('module_color_by_isa') ? (spieceasiConfig.module_color_by_isa as boolean) : false
    def networkModuleIsaSource = spieceasiConfig.module_isa_source ? spieceasiConfig.module_isa_source.toString().trim() : (networkIsaOverlayGroups ? networkIsaOverlayGroups[0] : indicspeciesGroup1)
    if( networkModuleIsaSource ==~ /^group\d+$/ ) {
        def idx = networkModuleIsaSource.replaceFirst(/^group/, '') as int
        if( idx >= 1 && idx <= indicspeciesGroupCols.size() ) {
            networkModuleIsaSource = indicspeciesGroupCols[idx - 1]
        }
    }
    if( !indicspeciesGroupCols.contains(networkModuleIsaSource) ) {
        networkModuleIsaSource = indicspeciesGroupCols ? indicspeciesGroupCols[0] : 'group1'
    }
    def networkModuleIsaMinStat = spieceasiConfig.module_isa_min_stat != null ? (spieceasiConfig.module_isa_min_stat as double) : 0.25d
    def networkModuleIsaMaxQ = spieceasiConfig.module_isa_max_q != null ? (spieceasiConfig.module_isa_max_q as double) : 0.05d
    def networkMetadataPath = spieceasiConfig.metadata ? resolveOptionalPath(spieceasiConfig.metadata, configRoot) : metadataPlotsMetadataPath
    def networkColorCol = spieceasiConfig.color_col ?: indicspeciesColorCol
    def networkGroupPaletteMap = new LinkedHashMap<String,String>(indicspeciesGroupPaletteMap)
    networkGroupPaletteMap.putAll(extractNamedStringMap(spieceasiConfig as Map, indicspeciesGroupCols, 'group_palettes', 'palette'))
    def networkGroupOrderMap = new LinkedHashMap<String,List<String>>(indicspeciesGroupOrderMap)
    networkGroupOrderMap.putAll(extractNamedListMap(spieceasiConfig as Map, indicspeciesGroupCols, 'group_orders', 'order'))
    def networkFocusLabelMap = new LinkedHashMap<String,String>(indicspeciesFocusLabelMap)
    networkFocusLabelMap.putAll(extractNamedStringMap(spieceasiConfig as Map, indicspeciesGroupCols, 'focus_labels', 'focus_label'))
    def networkGroup1Palette = indicspeciesGroup1 ? (networkGroupPaletteMap[indicspeciesGroup1] ?: '') : ''
    def networkGroup2Palette = indicspeciesGroup2 ? (networkGroupPaletteMap[indicspeciesGroup2] ?: '') : ''
    List<String> networkGroup1Order = indicspeciesGroup1 ? (networkGroupOrderMap[indicspeciesGroup1] ?: []) : []
    List<String> networkGroup2Order = indicspeciesGroup2 ? (networkGroupOrderMap[indicspeciesGroup2] ?: []) : []
    def networkFocusGroup1Label = indicspeciesGroup1 ? (networkFocusLabelMap[indicspeciesGroup1] ?: '') : ''
    def networkFocusGroup2Label = indicspeciesGroup2 ? (networkFocusLabelMap[indicspeciesGroup2] ?: '') : ''
    def networkIsaOverlayGroupsCsv = networkIsaOverlayGroups.join(',')
    def networkGroupPaletteJson = groovy.json.JsonOutput.toJson(networkGroupPaletteMap)
    def networkGroupOrderJson = groovy.json.JsonOutput.toJson(networkGroupOrderMap)
    def networkFocusLabelJson = groovy.json.JsonOutput.toJson(networkFocusLabelMap)
    boolean networkModulesEnabled = networkEnabled && (spieceasiConfig.containsKey('modules_enabled') ? (spieceasiConfig.modules_enabled as boolean) : false)
    def networkModuleMethodsRaw = spieceasiConfig.module_methods ?: 'leiden,louvain'
    List<String> networkModuleMethods = []
    if( networkModuleMethodsRaw instanceof List ) {
        networkModuleMethods = networkModuleMethodsRaw.collect { it.toString().trim().toLowerCase() }.findAll { it }
    } else if( networkModuleMethodsRaw ) {
        networkModuleMethods = networkModuleMethodsRaw.toString().split(/[,|]/).collect { it.trim().toLowerCase() }.findAll { it }
    }
    if( networkModuleMethods.isEmpty() ) {
        networkModuleMethods = ['leiden','louvain']
    }
    def networkModulePrimaryMethod = spieceasiConfig.module_primary_method ? spieceasiConfig.module_primary_method.toString().trim().toLowerCase() : networkModuleMethods[0]
    if( !networkModuleMethods.contains(networkModulePrimaryMethod) ) {
        networkModulePrimaryMethod = networkModuleMethods[0]
    }
    def networkModuleResolutionsRaw = spieceasiConfig.module_resolutions ?: '0.5,1.0,1.5'
    List<String> networkModuleResolutions = []
    if( networkModuleResolutionsRaw instanceof List ) {
        networkModuleResolutions = networkModuleResolutionsRaw.collect { it.toString().trim() }.findAll { it }
    } else if( networkModuleResolutionsRaw ) {
        networkModuleResolutions = networkModuleResolutionsRaw.toString().split(/[,|]/).collect { it.trim() }.findAll { it }
    }
    if( networkModuleResolutions.isEmpty() ) {
        networkModuleResolutions = ['1.0']
    }
    def networkModuleReps = spieceasiConfig.module_reps ? (spieceasiConfig.module_reps as int) : 25
    def networkModuleConsensusThreshold = spieceasiConfig.module_consensus_threshold != null ? (spieceasiConfig.module_consensus_threshold as double) : 0.8d
    def networkModuleSeed = spieceasiConfig.module_seed ? (spieceasiConfig.module_seed as int) : networkLayoutSeed
    def networkModulesSubPath = spieceasiConfig.modules_sub ? resolveOptionalPath(spieceasiConfig.modules_sub, configRoot) : new File(spieceasiOutputDirAbs, "${spieceasiPrefix}_modules_sub.tsv").canonicalPath
    def networkModulesAllPath = spieceasiConfig.modules_all ? resolveOptionalPath(spieceasiConfig.modules_all, configRoot) : new File(spieceasiOutputDirAbs, "${spieceasiPrefix}_modules_all.tsv").canonicalPath

    def masterSummaryConfig = config.master_summary ?: [:]
    boolean masterSummaryEnabled = masterSummaryConfig.containsKey('enabled') ? (masterSummaryConfig.enabled as boolean) : false
    if( masterSummaryEnabled && !metadataPlotsEnabled ) {
        exit 1, "master_summary.enabled requires metadata_plots.enabled to be true"
    }
    def masterSummaryOutputDir = masterSummaryConfig.output_dir ?: 'summary/tables'
    def masterSummaryOutputDirAbs = resolveOutputRelative(masterSummaryOutputDir.toString(), outputDir)
    def masterSummaryClustermapsDir = masterSummaryConfig.clustermaps_dir ?: 'clustermaps'
    def masterSummaryClustermapsDirAbs = resolveOutputRelative(masterSummaryClustermapsDir.toString(), outputDir)
    def masterSummaryIndicspeciesDir = masterSummaryConfig.indicspecies_dir ?: 'indicspecies'
    def masterSummaryIndicspeciesDirAbs = resolveOutputRelative(masterSummaryIndicspeciesDir.toString(), outputDir)
    def masterSummarySpieceasiDir = masterSummaryConfig.spieceasi_dir ?: 'spieceasi'
    def masterSummarySpieceasiDirAbs = resolveOutputRelative(masterSummarySpieceasiDir.toString(), outputDir)
    def masterSummaryAsvMagDir = masterSummaryConfig.asv_mag_dir ?: 'asv_mag_link'
    def masterSummaryAsvMagDirAbs = resolveOutputRelative(masterSummaryAsvMagDir.toString(), outputDir)
    def masterSummaryWhitelistRaw = masterSummaryConfig.whitelist
    List<String> masterSummaryWhitelist = []
    if( masterSummaryWhitelistRaw instanceof List ) {
        masterSummaryWhitelist = masterSummaryWhitelistRaw.collect { it.toString().trim() }.findAll { it }
    } else if( masterSummaryWhitelistRaw ) {
        masterSummaryWhitelist = masterSummaryWhitelistRaw.toString().split(/[,|]/).collect { it.trim() }.findAll { it }
    }
    def masterSummaryWhitelistCsv = masterSummaryWhitelist ? masterSummaryWhitelist.join(',') : ''
    def masterSummaryMaxDirectCols = masterSummaryConfig.max_direct_cols ? (masterSummaryConfig.max_direct_cols as int) : 300

    def asvMagLinkConfig = config.asv_mag_link ?: [:]
    boolean asvMagLinkEnabled = asvMagLinkConfig.containsKey('enabled') ? (asvMagLinkConfig.enabled as boolean) : false
    def asvMagLinkMasterTsv = asvMagLinkConfig.master_tsv ? resolveOptionalPath(asvMagLinkConfig.master_tsv, configRoot) : null
    def asvMagLinkBarrnapDir = asvMagLinkConfig.barrnap_dir ? resolveOptionalPath(asvMagLinkConfig.barrnap_dir, configRoot) : null
    def asvMagLinkGenomeDir = asvMagLinkConfig.genome_fasta_dir ? resolveOptionalPath(asvMagLinkConfig.genome_fasta_dir, configRoot) : null
    def asvMagLinkGenomeQcDir = asvMagLinkConfig.genome_qc_dir ? resolveOptionalPath(asvMagLinkConfig.genome_qc_dir, configRoot) : null
    def asvMagLinkGenomeQcDirsRaw = asvMagLinkConfig.genome_qc_dirs
    def asvMagLinkIdTokenIndexesRaw = asvMagLinkConfig.id_token_indexes
    List asvMagLinkGenomeQcDirs = []
    List asvMagLinkIdTokenIndexes = []
    if( asvMagLinkGenomeQcDirsRaw instanceof List ) {
        asvMagLinkGenomeQcDirs = asvMagLinkGenomeQcDirsRaw.collect { resolveOptionalPath(it, configRoot) }.findAll { it }
    } else if( asvMagLinkGenomeQcDirsRaw ) {
        asvMagLinkGenomeQcDirs = asvMagLinkGenomeQcDirsRaw.toString().split(/[,|]/).collect { resolveOptionalPath(it.trim(), configRoot) }.findAll { it }
    }
    if( asvMagLinkIdTokenIndexesRaw instanceof List ) {
        asvMagLinkIdTokenIndexes = asvMagLinkIdTokenIndexesRaw.collect { it == null || it.toString().trim() == '' ? -1 : (it as int) }
    } else if( asvMagLinkIdTokenIndexesRaw ) {
        asvMagLinkIdTokenIndexes = asvMagLinkIdTokenIndexesRaw.toString().split(/[,|]/).collect { token ->
            def trimmed = token.trim()
            trimmed ? (trimmed as int) : -1
        }
    }
    def asvMagLinkOutputDir = asvMagLinkConfig.output_dir ?: 'asv_mag_link'
    def asvMagLinkOutputDirAbs = resolveOutputRelative(asvMagLinkOutputDir.toString(), outputDir)
    def asvMagLinkThreads = asvMagLinkConfig.threads ? (asvMagLinkConfig.threads as int) : pipelineThreads
    def asvMagLinkMinPident = asvMagLinkConfig.min_pident != null ? (asvMagLinkConfig.min_pident as double) : 97.0d
    def asvMagLinkMinQcov = asvMagLinkConfig.min_qcov != null ? (asvMagLinkConfig.min_qcov as double) : 90.0d
    def asvMagLinkTopN = asvMagLinkConfig.top_n ? (asvMagLinkConfig.top_n as int) : 5
    def asvMagLinkPlotTopN = asvMagLinkConfig.plot_top_n ? (asvMagLinkConfig.plot_top_n as int) : 20
    if( asvMagLinkEnabled && asvMagLinkMasterTsv && (asvMagLinkBarrnapDir || asvMagLinkGenomeDir || asvMagLinkGenomeQcDir || asvMagLinkGenomeQcDirs) ) {
        exit 1, "asv_mag_link.master_tsv cannot be combined with genome_qc_dir, genome_qc_dirs, barrnap_dir, or genome_fasta_dir"
    }
    if( asvMagLinkEnabled && !asvMagLinkMasterTsv && !asvMagLinkBarrnapDir && !asvMagLinkGenomeQcDir && !asvMagLinkGenomeQcDirs ) {
        exit 1, "asv_mag_link.enabled requires asv_mag_link.master_tsv, genome_qc_dir, genome_qc_dirs, or barrnap_dir"
    }

    def asvMagNetworkConfig = config.asv_mag_network ?: [:]
    boolean asvMagNetworkRequested = asvMagNetworkConfig.containsKey('enabled') ? (asvMagNetworkConfig.enabled as boolean) : false
    if( asvMagNetworkRequested && !networkEnabled ) {
        exit 1, "asv_mag_network.enabled requires network.enabled"
    }
    if( asvMagNetworkRequested && !asvMagLinkEnabled ) {
        exit 1, "asv_mag_network.enabled requires asv_mag_link.enabled"
    }
    boolean asvMagNetworkEnabled = asvMagNetworkRequested
    def asvMagNetworkOutputDir = asvMagNetworkConfig.output_dir ?: 'asv_mag_network'
    def asvMagNetworkOutputDirAbs = resolveOutputRelative(asvMagNetworkOutputDir.toString(), outputDir)
    def asvMagNetworkPrefix = asvMagNetworkConfig.prefix ?: 'asv_mag_network'
    def asvMagNetworkGraphVariant = asvMagNetworkConfig.graph_variant ? asvMagNetworkConfig.graph_variant.toString().trim().toLowerCase() : (spieceasiAllPosOnly ? 'all' : 'thresholded')
    if( !['all', 'thresholded'].contains(asvMagNetworkGraphVariant) ) {
        exit 1, "asv_mag_network.graph_variant must be one of: all, thresholded"
    }
    def asvMagNetworkMinPident = asvMagNetworkConfig.min_pident != null ? (asvMagNetworkConfig.min_pident as double) : 99.5d
    def asvMagNetworkMinQcov = asvMagNetworkConfig.min_qcov != null ? (asvMagNetworkConfig.min_qcov as double) : 100.0d
    def asvMagNetworkAsvTaxonomySource = asvMagNetworkConfig.asv_taxonomy_source ? asvMagNetworkConfig.asv_taxonomy_source.toString().trim() : 'ncbi'
    def asvMagNetworkMagTaxonomySource = asvMagNetworkConfig.mag_taxonomy_source ? asvMagNetworkConfig.mag_taxonomy_source.toString().trim() : 'gtdb'
    def asvMagNetworkMagAbundance = asvMagNetworkConfig.mag_abundance ? resolveOptionalPath(asvMagNetworkConfig.mag_abundance, configRoot) : null
    def asvMagNetworkMagIdMode = asvMagNetworkConfig.mag_id_mode ? asvMagNetworkConfig.mag_id_mode.toString().trim().toLowerCase() : 'exact'
    if( !['exact', 'suffix_after_double_underscore'].contains(asvMagNetworkMagIdMode) ) {
        exit 1, "asv_mag_network.mag_id_mode must be one of: exact, suffix_after_double_underscore"
    }
    def asvMagNetworkMagAbundanceFormat = asvMagNetworkConfig.mag_abundance_format ? asvMagNetworkConfig.mag_abundance_format.toString().trim().toLowerCase() : 'auto'
    if( !['auto', 'long', 'wide'].contains(asvMagNetworkMagAbundanceFormat) ) {
        exit 1, "asv_mag_network.mag_abundance_format must be one of: auto, long, wide"
    }
    def asvMagNetworkMagAbundanceGenomeCol = asvMagNetworkConfig.mag_abundance_genome_col ? asvMagNetworkConfig.mag_abundance_genome_col.toString().trim() : 'genome_id'
    def asvMagNetworkMagAbundanceSampleCol = asvMagNetworkConfig.mag_abundance_sample_col ? asvMagNetworkConfig.mag_abundance_sample_col.toString().trim() : 'sample_id'
    def asvMagNetworkMagAbundanceValueCol = asvMagNetworkConfig.mag_abundance_value_col ? asvMagNetworkConfig.mag_abundance_value_col.toString().trim() : 'read_count'
    def asvMagNetworkMinSharedSamples = asvMagNetworkConfig.min_shared_samples ? (asvMagNetworkConfig.min_shared_samples as int) : 5
    def asvMagNetworkAbundanceTransform = asvMagNetworkConfig.abundance_transform ? asvMagNetworkConfig.abundance_transform.toString().trim().toLowerCase() : 'log1p'
    if( !['none', 'log1p'].contains(asvMagNetworkAbundanceTransform) ) {
        exit 1, "asv_mag_network.abundance_transform must be one of: none, log1p"
    }
    def asvMagNetworkFunctionalModuleMinFraction = asvMagNetworkConfig.functional_module_min_fraction != null ? (asvMagNetworkConfig.functional_module_min_fraction as double) : 0.5d
    def asvMagNetworkFunctionalRaw = asvMagNetworkConfig.functional_annotations ?: []
    List asvMagNetworkFunctionalAnnotations = []
    if( asvMagNetworkFunctionalRaw instanceof List ) {
        asvMagNetworkFunctionalAnnotations = asvMagNetworkFunctionalRaw.collect { resolveOptionalPath(it, configRoot) }.findAll { it }
    } else if( asvMagNetworkFunctionalRaw ) {
        asvMagNetworkFunctionalAnnotations = asvMagNetworkFunctionalRaw.toString().split(/[,|]/).collect { resolveOptionalPath(it.trim(), configRoot) }.findAll { it }
    }

    def powerAnalysisConfig = config.group_power_analysis ?: (config.power_analysis ?: [:])
    boolean powerAnalysisRequested = powerAnalysisConfig.containsKey('enabled') ? (powerAnalysisConfig.enabled as boolean) : false
    if( powerAnalysisRequested && !metadataPlotsEnabled ) {
        exit 1, "power_analysis.enabled requires metadata_plots.enabled to be true"
    }
    boolean powerAnalysisEnabled = powerAnalysisRequested
    def powerAnalysisOutputDir = powerAnalysisConfig.output_dir ?: 'power_analysis'
    def powerAnalysisOutputDirAbs = resolveOutputRelative(powerAnalysisOutputDir.toString(), outputDir)
    def powerAnalysisSampleCol = powerAnalysisConfig.sample_col ?: metadataPlotsSampleCol
    def powerAnalysisPatientCol = powerAnalysisConfig.patient_col ? powerAnalysisConfig.patient_col.toString().trim() : 'Participant_ID'
    def powerAnalysisCaseCol = powerAnalysisConfig.case_col ? powerAnalysisConfig.case_col.toString().trim() : 'Case'
    def powerAnalysisTypeCol = powerAnalysisConfig.type_col ?: metadataPlotsTypeCol
    def powerAnalysisSampleSizesCancerRaw = powerAnalysisConfig.sample_sizes_cancer ?: '6,8,10,15,20,25,30'
    def powerAnalysisSampleSizesCancer = powerAnalysisSampleSizesCancerRaw instanceof List ?
        powerAnalysisSampleSizesCancerRaw.collect { it.toString().trim() }.findAll { it }.join(',') :
        powerAnalysisSampleSizesCancerRaw.toString().trim()
    def powerAnalysisSampleSizesStypeRaw = powerAnalysisConfig.sample_sizes_stype ?: '10,15,20,25,30,40,50'
    def powerAnalysisSampleSizesStype = powerAnalysisSampleSizesStypeRaw instanceof List ?
        powerAnalysisSampleSizesStypeRaw.collect { it.toString().trim() }.findAll { it }.join(',') :
        powerAnalysisSampleSizesStypeRaw.toString().trim()
    def powerAnalysisNSimulations = powerAnalysisConfig.n_simulations ? (powerAnalysisConfig.n_simulations as int) : 1000
    def powerAnalysisNPerm = powerAnalysisConfig.n_perm ? (powerAnalysisConfig.n_perm as int) : 199
    def powerAnalysisAlpha = powerAnalysisConfig.alpha != null ? (powerAnalysisConfig.alpha as double) : 0.05d
    def powerAnalysisSeed = powerAnalysisConfig.seed ? (powerAnalysisConfig.seed as int) : 42
    boolean powerAnalysisSkipEstimate = powerAnalysisConfig.containsKey('skip_estimate') ? (powerAnalysisConfig.skip_estimate as boolean) : false
    boolean powerAnalysisSkipPlot = powerAnalysisConfig.containsKey('skip_plot') ? (powerAnalysisConfig.skip_plot as boolean) : false
    def powerAnalysisTransform = powerAnalysisConfig.transform ? powerAnalysisConfig.transform.toString().trim().toLowerCase() : 'none'
    if( !['none', 'rclr'].contains(powerAnalysisTransform) ) {
        exit 1, "power_analysis.transform must be one of: none, rclr"
    }
    boolean powerAnalysisKeepContralateralInCancer = powerAnalysisConfig.containsKey('keep_contralateral_in_cancer') ? (powerAnalysisConfig.keep_contralateral_in_cancer as boolean) : false
    def powerAnalysisContralateralTypesRaw = powerAnalysisConfig.contralateral_sample_types ?: 'Lung Brush,BAL'
    def powerAnalysisContralateralTypes = powerAnalysisContralateralTypesRaw instanceof List ?
        powerAnalysisContralateralTypesRaw.collect { it.toString().trim() }.findAll { it }.join(',') :
        powerAnalysisContralateralTypesRaw.toString().trim()
    def powerAnalysisIndicspeciesDir = powerAnalysisConfig.indicspecies_dir ? resolveOptionalPath(powerAnalysisConfig.indicspecies_dir, configRoot) : indicspeciesOutputDirAbs

    def taxonomyPatientAwareConfig = config.taxonomy_group_association ?: (config.taxonomy_patient_aware ?: [:])
    boolean taxonomyPatientAwareRequested = taxonomyPatientAwareConfig.containsKey('enabled') ? (taxonomyPatientAwareConfig.enabled as boolean) : false
    if( taxonomyPatientAwareRequested && !metadataPlotsEnabled ) {
        exit 1, "taxonomy_patient_aware.enabled requires metadata_plots.enabled to be true"
    }
    boolean taxonomyPatientAwareEnabled = taxonomyPatientAwareRequested
    def taxonomyPatientAwareOutputDir = taxonomyPatientAwareConfig.output_dir ?: (config.taxonomy_group_association ? 'taxonomy_group_association' : 'taxonomy_patient_aware')
    def taxonomyPatientAwareOutputDirAbs = resolveOutputRelative(taxonomyPatientAwareOutputDir.toString(), outputDir)
    def taxonomyPatientAwareSampleCol = taxonomyPatientAwareConfig.sample_col ?: metadataPlotsSampleCol
    def taxonomyPatientAwarePatientCol = taxonomyPatientAwareConfig.subject_col ? taxonomyPatientAwareConfig.subject_col.toString().trim() : (taxonomyPatientAwareConfig.patient_col ? taxonomyPatientAwareConfig.patient_col.toString().trim() : 'Participant_ID')
    def taxonomyPatientAwareCaseCol = taxonomyPatientAwareConfig.comparison_col ? taxonomyPatientAwareConfig.comparison_col.toString().trim() : (taxonomyPatientAwareConfig.case_col ? taxonomyPatientAwareConfig.case_col.toString().trim() : 'Case')
    def taxonomyPatientAwareTypeCol = taxonomyPatientAwareConfig.group_col ?: (taxonomyPatientAwareConfig.type_col ?: metadataPlotsTypeCol)
    def taxonomyPatientAwareCountCol = taxonomyPatientAwareConfig.count_col ? taxonomyPatientAwareConfig.count_col.toString().trim() : 'count'
    def taxonomyPatientAwareTaxLevelsRaw = taxonomyPatientAwareConfig.tax_levels ?: 'Phylum,Family'
    def taxonomyPatientAwareTaxLevels = taxonomyPatientAwareTaxLevelsRaw instanceof List ?
        taxonomyPatientAwareTaxLevelsRaw.collect { it.toString().trim() }.findAll { it }.join(',') :
        taxonomyPatientAwareTaxLevelsRaw.toString().trim()
    def taxonomyPatientAwareSampleTypesRaw = taxonomyPatientAwareConfig.containsKey('sample_types') ? taxonomyPatientAwareConfig.sample_types : 'Oral Rinse,BAL,Lung Brush'
    def taxonomyPatientAwareSampleTypes = taxonomyPatientAwareSampleTypesRaw instanceof List ?
        taxonomyPatientAwareSampleTypesRaw.collect { it.toString().trim() }.findAll { it }.join(',') :
        taxonomyPatientAwareSampleTypesRaw.toString().trim()
    def taxonomyPatientAwareComparisonGroupsRaw = taxonomyPatientAwareConfig.comparison_groups ?: ''
    def taxonomyPatientAwareComparisonGroups = taxonomyPatientAwareComparisonGroupsRaw instanceof List ?
        taxonomyPatientAwareComparisonGroupsRaw.collect { it.toString().trim() }.findAll { it }.join(',') :
        taxonomyPatientAwareComparisonGroupsRaw.toString().trim()
    boolean taxonomyPatientAwareRunComparison = taxonomyPatientAwareConfig.containsKey('run_comparison') ?
        (taxonomyPatientAwareConfig.run_comparison as boolean) :
        (!config.taxonomy_group_association || taxonomyPatientAwareComparisonGroups.toString().trim().length() > 0)
    def taxonomyPatientAwareMinPrevalence = taxonomyPatientAwareConfig.min_prevalence != null ? (taxonomyPatientAwareConfig.min_prevalence as double) : 0.10d
    boolean taxonomyPatientAwareExcludeContralateral = taxonomyPatientAwareConfig.containsKey('exclude_contralateral_in_cancer') ? (taxonomyPatientAwareConfig.exclude_contralateral_in_cancer as boolean) : true
    def taxonomyPatientAwareContralateralCol = taxonomyPatientAwareConfig.contralateral_col ? taxonomyPatientAwareConfig.contralateral_col.toString().trim() : 'lung_status'
    def taxonomyPatientAwareCancerSiteCol = taxonomyPatientAwareConfig.cancer_site_col ? taxonomyPatientAwareConfig.cancer_site_col.toString().trim() : 'Cancer_Site'
    def taxonomyPatientAwareLungSideCol = taxonomyPatientAwareConfig.lung_side_col ? taxonomyPatientAwareConfig.lung_side_col.toString().trim() : 'lung_code'
    def taxonomyPatientAwareContralateralValue = taxonomyPatientAwareConfig.contralateral_value ? taxonomyPatientAwareConfig.contralateral_value.toString().trim() : 'Contralateral'
    def taxonomyPatientAwareContralateralTypesRaw = taxonomyPatientAwareConfig.contralateral_sample_types ?: 'Lung Brush,BAL'
    def taxonomyPatientAwareContralateralTypes = taxonomyPatientAwareContralateralTypesRaw instanceof List ?
        taxonomyPatientAwareContralateralTypesRaw.collect { it.toString().trim() }.findAll { it }.join(',') :
        taxonomyPatientAwareContralateralTypesRaw.toString().trim()
    boolean taxonomyPatientAwareSkipOmnibus = taxonomyPatientAwareConfig.containsKey('skip_omnibus') ? (taxonomyPatientAwareConfig.skip_omnibus as boolean) : false
    def taxonomyPatientAwareTransform = taxonomyPatientAwareConfig.transform ? taxonomyPatientAwareConfig.transform.toString().trim().toLowerCase() : 'none'
    if( !['none', 'rclr'].contains(taxonomyPatientAwareTransform) ) {
        exit 1, "taxonomy_patient_aware.transform must be one of: none, rclr"
    }
    def taxonomyPatientAwareAlpha = taxonomyPatientAwareConfig.alpha != null ? (taxonomyPatientAwareConfig.alpha as double) : 0.05d
    def taxonomyPatientAwareTopN = taxonomyPatientAwareConfig.top_n ? (taxonomyPatientAwareConfig.top_n as int) : 12
    def taxonomyPatientAwareTypePalette = taxonomyPatientAwareConfig.type_palette ?: (indicspeciesGroupPaletteMap[taxonomyPatientAwareTypeCol] ?: '')
    def taxonomyPatientAwareCasePalette = taxonomyPatientAwareConfig.case_palette ?: (indicspeciesGroupPaletteMap[taxonomyPatientAwareCaseCol] ?: '')

    def lungStatusAnalysisConfig = config.paired_group_contrast ?: (config.lung_status_analysis ?: [:])
    boolean lungStatusAnalysisRequested = lungStatusAnalysisConfig.containsKey('enabled') ? (lungStatusAnalysisConfig.enabled as boolean) : false
    if( lungStatusAnalysisRequested && !metadataPlotsEnabled ) {
        exit 1, "lung_status_analysis.enabled requires metadata_plots.enabled to be true"
    }
    boolean lungStatusAnalysisEnabled = lungStatusAnalysisRequested
    def lungStatusAnalysisOutputDir = lungStatusAnalysisConfig.output_dir ?: (config.paired_group_contrast ? 'paired_group_contrast' : 'lung_status_analysis')
    def lungStatusAnalysisOutputDirAbs = resolveOutputRelative(lungStatusAnalysisOutputDir.toString(), outputDir)
    def lungStatusAnalysisSampleCol = lungStatusAnalysisConfig.sample_col ?: metadataPlotsSampleCol
    def lungStatusAnalysisTypeCol = lungStatusAnalysisConfig.type_col ?: metadataPlotsTypeCol
    def lungStatusAnalysisSampleTypesRaw = lungStatusAnalysisConfig.containsKey('sample_types') ? lungStatusAnalysisConfig.sample_types : 'Lung Brush,BAL'
    def lungStatusAnalysisSampleTypes = lungStatusAnalysisSampleTypesRaw instanceof List ?
        lungStatusAnalysisSampleTypesRaw.collect { it.toString().trim() }.findAll { it }.join(',') :
        lungStatusAnalysisSampleTypesRaw.toString().trim()
    def lungStatusAnalysisCaseCol = lungStatusAnalysisConfig.case_col ? lungStatusAnalysisConfig.case_col.toString().trim() : 'Case'
    def lungStatusAnalysisPatientCol = lungStatusAnalysisConfig.subject_col ? lungStatusAnalysisConfig.subject_col.toString().trim() : (lungStatusAnalysisConfig.patient_col ? lungStatusAnalysisConfig.patient_col.toString().trim() : 'Participant_ID')
    def lungStatusAnalysisCancerSiteCol = lungStatusAnalysisConfig.cancer_site_col ? lungStatusAnalysisConfig.cancer_site_col.toString().trim() : 'Cancer_Site'
    def lungStatusAnalysisLungCodeCol = lungStatusAnalysisConfig.lung_code_col ? lungStatusAnalysisConfig.lung_code_col.toString().trim() : 'lung_code'
    def lungStatusAnalysisTumorSideCol = lungStatusAnalysisConfig.tumor_side_col ? lungStatusAnalysisConfig.tumor_side_col.toString().trim() : 'TumorSide'
    def lungStatusAnalysisContralateralCol = lungStatusAnalysisConfig.contralateral_col ? lungStatusAnalysisConfig.contralateral_col.toString().trim() : 'Contralateral'
    def lungStatusAnalysisHealthyCol = lungStatusAnalysisConfig.healthy_col ? lungStatusAnalysisConfig.healthy_col.toString().trim() : 'Healthy'
    def lungStatusAnalysisStatusCol = lungStatusAnalysisConfig.lung_status_col ? lungStatusAnalysisConfig.lung_status_col.toString().trim() : 'lung_status'
    def lungStatusAnalysisStatusAValue = lungStatusAnalysisConfig.status_a_value ? lungStatusAnalysisConfig.status_a_value.toString().trim() : 'TumorSide'
    def lungStatusAnalysisStatusBValue = lungStatusAnalysisConfig.status_b_value ? lungStatusAnalysisConfig.status_b_value.toString().trim() : 'Contralateral'
    def lungStatusAnalysisReferenceStatusValue = lungStatusAnalysisConfig.reference_status_value ? lungStatusAnalysisConfig.reference_status_value.toString().trim() : 'Healthy'
    def lungStatusAnalysisPermutations = lungStatusAnalysisConfig.permutations ? (lungStatusAnalysisConfig.permutations as int) : 9999
    def lungStatusAnalysisSeed = lungStatusAnalysisConfig.seed ? (lungStatusAnalysisConfig.seed as int) : 1
    return [
        metadataPlotsEnabled: metadataPlotsEnabled,
        metadataPlotsMetadataPath: metadataPlotsMetadataPath,
        metadataPlotsSampleCol: metadataPlotsSampleCol,
        metadataPlotsTypeCol: metadataPlotsTypeCol,
        metadataPlotsColorCol: metadataPlotsColorCol,
        metadataPlotsGroupOrder: metadataPlotsGroupOrder,
        indicspeciesSampleCol: indicspeciesSampleCol,
        indicspeciesPerms: indicspeciesPerms,
        indicspeciesSeed: indicspeciesSeed,
        indicspeciesQThreshold: indicspeciesQThreshold,
        indicspeciesMinN: indicspeciesMinN,
        indicspeciesBlockCol: indicspeciesBlockCol,
        indicspeciesStratifiedSpecsArg: indicspeciesStratifiedSpecsArg,
        indicspeciesGroup1: indicspeciesGroup1,
        indicspeciesGroup2: indicspeciesGroup2,
        indicspeciesOutputDirAbs: indicspeciesOutputDirAbs,
        indicspeciesPlotPairsMode: indicspeciesPlotPairsMode,
        indicspeciesPlotOutputDirAbs: indicspeciesPlotOutputDirAbs,
        indicspeciesPlotVennPath: indicspeciesPlotVennPath,
        indicspeciesPlotTaxonomyPath: indicspeciesPlotTaxonomyPath,
        indicspeciesColorCol: indicspeciesColorCol,
        indicspeciesGroupPaletteJson: indicspeciesGroupPaletteJson,
        indicspeciesGroupOrderJson: indicspeciesGroupOrderJson,
        indicspeciesFocusLabelJson: indicspeciesFocusLabelJson,
        indicspeciesAlignedOutputDirAbs: indicspeciesAlignedOutputDirAbs,
        indicspeciesAlignedAlpha: indicspeciesAlignedAlpha,
        indicspeciesAlignedMinStat: indicspeciesAlignedMinStat,
        indicspeciesAlignedTopN: indicspeciesAlignedTopN,
        vocCorrelationEnabled: vocCorrelationEnabled,
        vocCorrelationVocTablePath: vocCorrelationVocTablePath,
        vocCorrelationOutputDirAbs: vocCorrelationOutputDirAbs,
        vocCorrelationVocSampleCol: vocCorrelationVocSampleCol,
        vocCorrelationSampleIdMode: vocCorrelationSampleIdMode,
        vocCorrelationMetadataSampleCol: vocCorrelationMetadataSampleCol,
        vocCorrelationTypeCol: vocCorrelationTypeCol,
        vocCorrelationPatientCol: vocCorrelationPatientCol,
        vocCorrelationCaseCol: vocCorrelationCaseCol,
        vocCorrelationSampleTypes: vocCorrelationSampleTypes,
        vocCorrelationUseLegacySubset: vocCorrelationUseLegacySubset,
        vocCorrelationVocCols: vocCorrelationVocCols,
        vocCorrelationDirection: vocCorrelationDirection,
        vocCorrelationCasePalette: vocCorrelationCasePalette,
        vocCorrelationIsaPalette: vocCorrelationIsaPalette,
        measurementAssociationEnabled: measurementAssociationEnabled,
        measurementAssociationOutputDirAbs: measurementAssociationOutputDirAbs,
        measurementAssociationTablePath: measurementAssociationTablePath,
        measurementAssociationSampleCol: measurementAssociationSampleCol,
        measurementAssociationAsvIdCol: measurementAssociationAsvIdCol,
        measurementAssociationMeasurementSampleCol: measurementAssociationMeasurementSampleCol,
        measurementAssociationMetadataJoinCols: measurementAssociationMetadataJoinCols,
        measurementAssociationMeasurementJoinCols: measurementAssociationMeasurementJoinCols,
        measurementAssociationCols: measurementAssociationCols,
        measurementAssociationExcludeCols: measurementAssociationExcludeCols,
        measurementAssociationGroupCol: measurementAssociationGroupCol,
        measurementAssociationGroupPalette: measurementAssociationGroupPalette,
        measurementAssociationMaxAsvs: measurementAssociationMaxAsvs,
        measurementAssociationMinTotal: measurementAssociationMinTotal,
        measurementAssociationMinPrevalence: measurementAssociationMinPrevalence,
        measurementAssociationTopCorrelations: measurementAssociationTopCorrelations,
        measurementAssociationDirection: measurementAssociationDirection,
        measurementAssociationMethods: measurementAssociationMethods,
        measurementAssociationPermutations: measurementAssociationPermutations,
        measurementAssociationTopVectors: measurementAssociationTopVectors,
        measurementAssociationFormats: measurementAssociationFormats,
        groupingDiagnosticsEnabled: groupingDiagnosticsEnabled,
        groupingDiagnosticsOutputDirAbs: groupingDiagnosticsOutputDirAbs,
        groupingDiagnosticsSampleCol: groupingDiagnosticsSampleCol,
        groupingDiagnosticsGroupCols: groupingDiagnosticsGroupCols,
        groupingDiagnosticsBaselineGroup: groupingDiagnosticsBaselineGroup,
        groupingDiagnosticsPrimaryGroup: groupingDiagnosticsPrimaryGroup,
        groupingDiagnosticsPaletteJson: groupingDiagnosticsPaletteJson,
        groupingDiagnosticsOrderJson: groupingDiagnosticsOrderJson,
        groupingDiagnosticsMetrics: groupingDiagnosticsMetrics,
        groupingDiagnosticsTransform: groupingDiagnosticsTransform,
        groupingDiagnosticsPermutations: groupingDiagnosticsPermutations,
        groupingDiagnosticsRandomState: groupingDiagnosticsRandomState,
        groupingDiagnosticsFormats: groupingDiagnosticsFormats,
        groupingDiagnosticsSoftLabelEnabled: groupingDiagnosticsSoftLabelEnabled,
        groupingDiagnosticsSoftLabelK: groupingDiagnosticsSoftLabelK,
        groupingDiagnosticsSoftLabelTargetCols: groupingDiagnosticsSoftLabelTargetCols,
        groupingDiagnosticsSoftLabelExcludeLabels: groupingDiagnosticsSoftLabelExcludeLabels,
        groupingDiagnosticsSoftLabelMinClassSamples: groupingDiagnosticsSoftLabelMinClassSamples,
        groupingDiagnosticsSoftLabelDistanceQuantile: groupingDiagnosticsSoftLabelDistanceQuantile,
        groupingDiagnosticsApplySoftLabels: groupingDiagnosticsApplySoftLabels,
        groupingDiagnosticsSoftLabelTargetCol: groupingDiagnosticsSoftLabelTargetCol,
        groupingDiagnosticsSoftLabelMinConfidence: groupingDiagnosticsSoftLabelMinConfidence,
        groupingDiagnosticsSoftLabelMinNeighborAgreement: groupingDiagnosticsSoftLabelMinNeighborAgreement,
        groupingDiagnosticsSoftLabelMinCvBalancedAccuracy: groupingDiagnosticsSoftLabelMinCvBalancedAccuracy,
        groupingDiagnosticsPowerEnabled: groupingDiagnosticsPowerEnabled,
        groupingDiagnosticsPowerSizes: groupingDiagnosticsPowerSizes,
        groupingDiagnosticsPowerSimulations: groupingDiagnosticsPowerSimulations,
        groupingDiagnosticsPowerPermutations: groupingDiagnosticsPowerPermutations,
        groupingDiagnosticsPowerAlpha: groupingDiagnosticsPowerAlpha,
        groupingDiagnosticsPowerMinGroups: groupingDiagnosticsPowerMinGroups,
        clustermapsOutputDirAbs: clustermapsOutputDirAbs,
        clustermapsMitoOutputDirAbs: clustermapsMitoOutputDirAbs,
        clustermapsMitoInputPath: clustermapsMitoInputPath,
        clustermapsIsaFile: clustermapsIsaFile,
        clustermapsIsaSearchDir: clustermapsIsaSearchDir,
        clustermapsSampleCol: clustermapsSampleCol,
        clustermapsSampleCodeCol: clustermapsSampleCodeCol,
        clustermapsAsvIdCol: clustermapsAsvIdCol,
        clustermapsGroup1Col: clustermapsGroup1Col,
        clustermapsGroup2Col: clustermapsGroup2Col,
        clustermapsGroup3Col: clustermapsGroup3Col,
        clustermapsExcludeGroup1: clustermapsExcludeGroup1,
        clustermapsGroup1Palette: clustermapsGroup1Palette,
        clustermapsGroup2Palette: clustermapsGroup2Palette,
        clustermapsGroup3Palette: clustermapsGroup3Palette,
        clustermapsRanks: clustermapsRanks,
        clustermapsTopN: clustermapsTopN,
        clustermapsCountCol: clustermapsCountCol,
        clustermapsIsaMinStat: clustermapsIsaMinStat,
        clustermapsIsaSignificanceCols: clustermapsIsaSignificanceCols,
        clustermapsIsaStatCols: clustermapsIsaStatCols,
        clustermapsFormats: clustermapsFormats,
        clustermapsFigWidth: clustermapsFigWidth,
        clustermapsRowHeight: clustermapsRowHeight,
        clustermapsMinHeight: clustermapsMinHeight,
        clustermapsMaxHeight: clustermapsMaxHeight,
        clustermapsMitoSampleMode: clustermapsMitoSampleMode,
        clustermapsIsaAutoCandidates: clustermapsIsaAutoCandidates,
        spieceasiOutputDirAbs: spieceasiOutputDirAbs,
        spieceasiPrefix: spieceasiPrefix,
        spieceasiMinRelAbund: spieceasiMinRelAbund,
        spieceasiMinPrevalence: spieceasiMinPrevalence,
        spieceasiMethod: spieceasiMethod,
        spieceasiLambdaMinRatio: spieceasiLambdaMinRatio,
        spieceasiNlambda: spieceasiNlambda,
        spieceasiRepNum: spieceasiRepNum,
        spieceasiThresh: spieceasiThresh,
        spieceasiPulsarCriterion: spieceasiPulsarCriterion,
        spieceasiNcores: spieceasiNcores,
        spieceasiSeed: spieceasiSeed,
        spieceasiEdgeThreshold: spieceasiEdgeThreshold,
        spieceasiLayoutIters: spieceasiLayoutIters,
        networkGraphAllPath: networkGraphAllPath,
        networkGraphThrPath: networkGraphThrPath,
        networkNodeFeaturesPath: networkNodeFeaturesPath,
        networkLayoutSeed: networkLayoutSeed,
        networkLayoutScale: networkLayoutScale,
        networkDegreeScale: networkDegreeScale,
        networkDegreeSizeMode: networkDegreeSizeMode,
        networkDegreeMinArea: networkDegreeMinArea,
        networkEdgeWidthScale: networkEdgeWidthScale,
        networkIsaScale: networkIsaScale,
        networkAbundanceSizeMode: networkAbundanceSizeMode,
        networkAbundanceReference: networkAbundanceReference,
        networkAbundanceReferenceArea: networkAbundanceReferenceArea,
        networkAbundanceMinArea: networkAbundanceMinArea,
        networkAbundanceMaxArea: networkAbundanceMaxArea,
        networkAbundanceScalePower: networkAbundanceScalePower,
        networkModuleBestMinSize: networkModuleBestMinSize,
        networkModuleBestMinStability: networkModuleBestMinStability,
        networkModuleIsaSource: networkModuleIsaSource,
        networkModuleIsaMinStat: networkModuleIsaMinStat,
        networkModuleIsaMaxQ: networkModuleIsaMaxQ,
        networkMetadataPath: networkMetadataPath,
        networkColorCol: networkColorCol,
        networkIsaOverlayGroupsCsv: networkIsaOverlayGroupsCsv,
        networkGroupPaletteJson: networkGroupPaletteJson,
        networkGroupOrderJson: networkGroupOrderJson,
        networkFocusLabelJson: networkFocusLabelJson,
        networkModulePrimaryMethod: networkModulePrimaryMethod,
        networkModuleReps: networkModuleReps,
        networkModuleConsensusThreshold: networkModuleConsensusThreshold,
        networkModuleSeed: networkModuleSeed,
        networkModulesSubPath: networkModulesSubPath,
        networkModulesAllPath: networkModulesAllPath,
        masterSummaryOutputDirAbs: masterSummaryOutputDirAbs,
        masterSummaryClustermapsDirAbs: masterSummaryClustermapsDirAbs,
        masterSummaryIndicspeciesDirAbs: masterSummaryIndicspeciesDirAbs,
        masterSummarySpieceasiDirAbs: masterSummarySpieceasiDirAbs,
        masterSummaryAsvMagDirAbs: masterSummaryAsvMagDirAbs,
        masterSummaryWhitelistCsv: masterSummaryWhitelistCsv,
        masterSummaryMaxDirectCols: masterSummaryMaxDirectCols,
        asvMagLinkBarrnapDir: asvMagLinkBarrnapDir,
        asvMagLinkMasterTsv: asvMagLinkMasterTsv,
        asvMagLinkGenomeDir: asvMagLinkGenomeDir,
        asvMagLinkGenomeQcDir: asvMagLinkGenomeQcDir,
        asvMagLinkOutputDirAbs: asvMagLinkOutputDirAbs,
        asvMagLinkThreads: asvMagLinkThreads,
        asvMagLinkMinPident: asvMagLinkMinPident,
        asvMagLinkMinQcov: asvMagLinkMinQcov,
        asvMagLinkTopN: asvMagLinkTopN,
        asvMagLinkPlotTopN: asvMagLinkPlotTopN,
        asvMagNetworkOutputDirAbs: asvMagNetworkOutputDirAbs,
        asvMagNetworkPrefix: asvMagNetworkPrefix,
        asvMagNetworkGraphVariant: asvMagNetworkGraphVariant,
        asvMagNetworkMinPident: asvMagNetworkMinPident,
        asvMagNetworkMinQcov: asvMagNetworkMinQcov,
        asvMagNetworkAsvTaxonomySource: asvMagNetworkAsvTaxonomySource,
        asvMagNetworkMagTaxonomySource: asvMagNetworkMagTaxonomySource,
        asvMagNetworkMagAbundance: asvMagNetworkMagAbundance,
        asvMagNetworkMagIdMode: asvMagNetworkMagIdMode,
        asvMagNetworkMagAbundanceFormat: asvMagNetworkMagAbundanceFormat,
        asvMagNetworkMagAbundanceGenomeCol: asvMagNetworkMagAbundanceGenomeCol,
        asvMagNetworkMagAbundanceSampleCol: asvMagNetworkMagAbundanceSampleCol,
        asvMagNetworkMagAbundanceValueCol: asvMagNetworkMagAbundanceValueCol,
        asvMagNetworkMinSharedSamples: asvMagNetworkMinSharedSamples,
        asvMagNetworkAbundanceTransform: asvMagNetworkAbundanceTransform,
        asvMagNetworkFunctionalModuleMinFraction: asvMagNetworkFunctionalModuleMinFraction,
        asvMagNetworkFunctionalAnnotations: asvMagNetworkFunctionalAnnotations,
        powerAnalysisOutputDirAbs: powerAnalysisOutputDirAbs,
        powerAnalysisSampleCol: powerAnalysisSampleCol,
        powerAnalysisPatientCol: powerAnalysisPatientCol,
        powerAnalysisCaseCol: powerAnalysisCaseCol,
        powerAnalysisTypeCol: powerAnalysisTypeCol,
        powerAnalysisSampleSizesCancer: powerAnalysisSampleSizesCancer,
        powerAnalysisSampleSizesStype: powerAnalysisSampleSizesStype,
        powerAnalysisNSimulations: powerAnalysisNSimulations,
        powerAnalysisNPerm: powerAnalysisNPerm,
        powerAnalysisAlpha: powerAnalysisAlpha,
        powerAnalysisSeed: powerAnalysisSeed,
        powerAnalysisTransform: powerAnalysisTransform,
        powerAnalysisContralateralTypes: powerAnalysisContralateralTypes,
        powerAnalysisIndicspeciesDir: powerAnalysisIndicspeciesDir,
        taxonomyPatientAwareOutputDirAbs: taxonomyPatientAwareOutputDirAbs,
        taxonomyPatientAwareSampleCol: taxonomyPatientAwareSampleCol,
        taxonomyPatientAwarePatientCol: taxonomyPatientAwarePatientCol,
        taxonomyPatientAwareCaseCol: taxonomyPatientAwareCaseCol,
        taxonomyPatientAwareTypeCol: taxonomyPatientAwareTypeCol,
        taxonomyPatientAwareCountCol: taxonomyPatientAwareCountCol,
        taxonomyPatientAwareTaxLevels: taxonomyPatientAwareTaxLevels,
        taxonomyPatientAwareSampleTypes: taxonomyPatientAwareSampleTypes,
        taxonomyPatientAwareComparisonGroups: taxonomyPatientAwareComparisonGroups,
        taxonomyPatientAwareRunComparison: taxonomyPatientAwareRunComparison,
        taxonomyPatientAwareMinPrevalence: taxonomyPatientAwareMinPrevalence,
        taxonomyPatientAwareContralateralCol: taxonomyPatientAwareContralateralCol,
        taxonomyPatientAwareCancerSiteCol: taxonomyPatientAwareCancerSiteCol,
        taxonomyPatientAwareLungSideCol: taxonomyPatientAwareLungSideCol,
        taxonomyPatientAwareContralateralValue: taxonomyPatientAwareContralateralValue,
        taxonomyPatientAwareContralateralTypes: taxonomyPatientAwareContralateralTypes,
        taxonomyPatientAwareTransform: taxonomyPatientAwareTransform,
        taxonomyPatientAwareAlpha: taxonomyPatientAwareAlpha,
        taxonomyPatientAwareTopN: taxonomyPatientAwareTopN,
        taxonomyPatientAwareTypePalette: taxonomyPatientAwareTypePalette,
        taxonomyPatientAwareCasePalette: taxonomyPatientAwareCasePalette,
        lungStatusAnalysisOutputDirAbs: lungStatusAnalysisOutputDirAbs,
        lungStatusAnalysisSampleCol: lungStatusAnalysisSampleCol,
        lungStatusAnalysisTypeCol: lungStatusAnalysisTypeCol,
        lungStatusAnalysisSampleTypes: lungStatusAnalysisSampleTypes,
        lungStatusAnalysisCaseCol: lungStatusAnalysisCaseCol,
        lungStatusAnalysisPatientCol: lungStatusAnalysisPatientCol,
        lungStatusAnalysisCancerSiteCol: lungStatusAnalysisCancerSiteCol,
        lungStatusAnalysisLungCodeCol: lungStatusAnalysisLungCodeCol,
        lungStatusAnalysisTumorSideCol: lungStatusAnalysisTumorSideCol,
        lungStatusAnalysisContralateralCol: lungStatusAnalysisContralateralCol,
        lungStatusAnalysisHealthyCol: lungStatusAnalysisHealthyCol,
        lungStatusAnalysisStatusCol: lungStatusAnalysisStatusCol,
        lungStatusAnalysisStatusAValue: lungStatusAnalysisStatusAValue,
        lungStatusAnalysisStatusBValue: lungStatusAnalysisStatusBValue,
        lungStatusAnalysisReferenceStatusValue: lungStatusAnalysisReferenceStatusValue,
        lungStatusAnalysisPermutations: lungStatusAnalysisPermutations,
        lungStatusAnalysisSeed: lungStatusAnalysisSeed,
        indicspeciesGroupCols: indicspeciesGroupCols,
        indicspeciesEnabled: indicspeciesEnabled,
        indicspeciesPlotEnabled: indicspeciesPlotEnabled,
        indicspeciesLabelFocusedAsvs: indicspeciesLabelFocusedAsvs,
        indicspeciesAlignedEnabled: indicspeciesAlignedEnabled,
        indicspeciesUseDuleg: indicspeciesUseDuleg,
        clustermapsEnabled: clustermapsEnabled,
        clustermapsGroup1Order: clustermapsGroup1Order,
        clustermapsRunMito: clustermapsRunMito,
        spieceasiEnabled: spieceasiEnabled,
        spieceasiTranspose: spieceasiTranspose,
        spieceasiRemoveZeroVar: spieceasiRemoveZeroVar,
        spieceasiKeepNegative: spieceasiKeepNegative,
        spieceasiAllPosOnly: spieceasiAllPosOnly,
        spieceasiForceFilter: spieceasiForceFilter,
        spieceasiForceSpieceasi: spieceasiForceSpieceasi,
        spieceasiForceGraphs: spieceasiForceGraphs,
        networkEnabled: networkEnabled,
        networkModes: networkModes,
        networkModuleBestOnly: networkModuleBestOnly,
        networkModuleIsaOnly: networkModuleIsaOnly,
        networkModuleColorByIsa: networkModuleColorByIsa,
        networkModulesEnabled: networkModulesEnabled,
        networkModuleMethods: networkModuleMethods,
        networkModuleResolutions: networkModuleResolutions,
        masterSummaryEnabled: masterSummaryEnabled,
        asvMagLinkEnabled: asvMagLinkEnabled,
        asvMagNetworkEnabled: asvMagNetworkEnabled,
        asvMagLinkGenomeQcDirs: asvMagLinkGenomeQcDirs,
        asvMagLinkIdTokenIndexes: asvMagLinkIdTokenIndexes,
        powerAnalysisEnabled: powerAnalysisEnabled,
        powerAnalysisSkipEstimate: powerAnalysisSkipEstimate,
        powerAnalysisSkipPlot: powerAnalysisSkipPlot,
        powerAnalysisKeepContralateralInCancer: powerAnalysisKeepContralateralInCancer,
        taxonomyPatientAwareEnabled: taxonomyPatientAwareEnabled,
        taxonomyPatientAwareExcludeContralateral: taxonomyPatientAwareExcludeContralateral,
        taxonomyPatientAwareSkipOmnibus: taxonomyPatientAwareSkipOmnibus,
        lungStatusAnalysisEnabled: lungStatusAnalysisEnabled,
    ]
}

workflow {
    def rawReadsForAsv = raw_reads
    def fastp_result = FASTP_QC(rawReadsForAsv)
    def reads_after_qc = fastp_result.reads
    def reads_after_merge = MERGE_READS(reads_after_qc)
    def reads_after_filter = FILTER_READS(reads_after_merge)
    def reads_for_concat = reads_after_filter
    if( concatRelabelEnabled ) {
        def relabeled_stage = RELABEL_FILTERED(reads_after_filter)
        reads_for_concat = relabeled_stage.relabeled
    }
    def relabeled_fasta_files = reads_for_concat.map { parts -> parts[1] }
    def concat_stage = CONCAT_FASTAS(relabeled_fasta_files.collect())
    def concat_for_derep = concat_stage.concat_for_derep
    def concat_for_counts = concat_stage.concat_for_counts

    def derep_input = DEREPLICATE(concat_for_derep)
    def denoise_input = DENOISE(derep_input)
    def nochi_input = CHIMERA_CHECK(denoise_input)

    def count_matrix_stage = CREATE_COUNT_MATRIX(concat_for_counts, nochi_input)
    def count_matrix_channel = count_matrix_stage.count_matrix
    def asv_counts_for_sankey = count_matrix_channel.map { tuple -> tuple[0] }
    def filtered_stage = FILTER_TABLE(count_matrix_channel)
    def filtered_channel = filtered_stage.filtered
    def filtered_fasta_for_taxonomy = filtered_channel.map { tuple -> tuple[1] }
    def sina_stage = SINA_TRIM(filtered_fasta_for_taxonomy)
    def taxonomy_stage = TAXONOMY(sina_stage.trimmed_fasta)
    def runMitoStages = mitoEnabled || filterCountsEnabled
    def filter_counts_stage = null
    if( runMitoStages ) {
        def blast_database_stage = PREPARE_BLAST_DATABASES(
            mitoBlastFastaPath ?: mitoBlastDbPath,
            mitoBiofFastaPath ?: mitoBiofDbPath
        )
        def mitomaster_stage = MITOMASTER(filtered_channel, blast_database_stage.databases)
        def mito_summary = MITO_DECONTAM(mitomaster_stage.mito_artifacts, taxonomy_stage.taxonomy_table)
        if( filterCountsEnabled ) {
            filter_counts_stage = FILTER_COUNTS(filtered_channel, taxonomy_stage.taxonomy_table, mito_summary.nontarget_table)
        }
    }
    if( metadataPlotsEnabled && filter_counts_stage == null ) {
        exit 1, "metadata_plots.enabled requires filter_counts outputs but filter_counts stage was not executed"
    }
    def general_stats_stage = null
    if( generalStatsEnabled ) {
        general_stats_stage = GENERAL_STATS(concat_for_counts)
    }

    def metadata_analysis_stage = null
    def metaMicroForNetwork = null
    def asvFinalForSpieceasi = null
    def asvFinalForNetwork = null
    def asvMetaForMasterSummary = null
    def asvFinalForMasterSummary = null
    def indicspeciesTablesForOverlay = Channel.value(file(emptyModulesPath))
    def indicspeciesGroup1SummaryForSpieceasi = Channel.value(file(emptyModulesPath))
    if( metadataPlotsEnabled ) {
        metadata_analysis_stage = RUN_METADATA_ANALYSES(
            general_stats_stage.fastq_stats,
            filter_counts_stage.filtered_counts,
            filter_counts_stage.filtered_mito,
            taxonomy_stage.taxonomy_table
        )
        metaMicroForNetwork = metadata_analysis_stage.meta_micro_network
        asvFinalForSpieceasi = metadata_analysis_stage.asv_final_spieceasi
        asvFinalForNetwork = metadata_analysis_stage.asv_final_network
        asvMetaForMasterSummary = metadata_analysis_stage.asv_meta_master_summary
        asvFinalForMasterSummary = metadata_analysis_stage.asv_final_master_summary
        indicspeciesTablesForOverlay = metadata_analysis_stage.indicspecies_tables
        indicspeciesGroup1SummaryForSpieceasi = metadata_analysis_stage.indicspecies_group1_summary
    }
    def asv_mag_link_stage = null
    if( asvMagLinkEnabled ) {
        asv_mag_link_stage = ASV_MAG_LINK(
            filtered_fasta_for_taxonomy
        )
    }
    def spieceasi_stage = null
    def graphAllForModules = null
    def graphAllForNetwork = null
    def graphAllForAsvMagNetwork = null
    def graphThrForModules = null
    def graphThrForNetwork = null
    def graphThrForAsvMagNetwork = null
    def nodeFeaturesForNetwork = null
    def nodeFeaturesForAsvMagNetwork = null
    def modulesSubForNetwork = null
    def modulesAllForNetwork = null
    if( spieceasiEnabled ) {
        spieceasi_stage = SPIECEASI(
            asvFinalForSpieceasi,
            indicspeciesGroup1SummaryForSpieceasi
        )
        graphAllForModules = spieceasi_stage.graph_all.map { it }
        graphAllForNetwork = spieceasi_stage.graph_all.map { it }
        graphAllForAsvMagNetwork = spieceasi_stage.graph_all.map { it }
        graphThrForModules = spieceasiAllPosOnly ? spieceasi_stage.graph_all.map { it } : spieceasi_stage.graph_thr.map { it }
        graphThrForNetwork = spieceasiAllPosOnly ? spieceasi_stage.graph_all.map { it } : spieceasi_stage.graph_thr.map { it }
        graphThrForAsvMagNetwork = spieceasiAllPosOnly ? spieceasi_stage.graph_all.map { it } : spieceasi_stage.graph_thr.map { it }
        nodeFeaturesForNetwork = spieceasi_stage.node_features
        nodeFeaturesForAsvMagNetwork = spieceasi_stage.node_features.map { it }
    } else if( networkEnabled ) {
        graphAllForModules = Channel.value(file(networkGraphAllPath))
        graphAllForNetwork = Channel.value(file(networkGraphAllPath))
        graphAllForAsvMagNetwork = Channel.value(file(networkGraphAllPath))
        graphThrForModules = Channel.value(file(spieceasiAllPosOnly ? networkGraphAllPath : networkGraphThrPath))
        graphThrForNetwork = Channel.value(file(spieceasiAllPosOnly ? networkGraphAllPath : networkGraphThrPath))
        graphThrForAsvMagNetwork = Channel.value(file(spieceasiAllPosOnly ? networkGraphAllPath : networkGraphThrPath))
        nodeFeaturesForNetwork = Channel.value(file(networkNodeFeaturesPath))
        nodeFeaturesForAsvMagNetwork = Channel.value(file(networkNodeFeaturesPath))
    }
    if( networkEnabled ) {
        if( networkModulesEnabled ) {
            def network_modules_stage = NETWORK_MODULES(
                graphAllForModules,
                graphThrForModules
            )
            modulesSubForNetwork = network_modules_stage.modules_sub
            modulesAllForNetwork = network_modules_stage.modules_all
        } else {
            def modulesSubFile = new File(networkModulesSubPath)
            def modulesAllFile = new File(networkModulesAllPath)
            modulesSubForNetwork = modulesSubFile.exists() ? Channel.value(file(networkModulesSubPath)) : Channel.value(file(emptyModulesPath))
            modulesAllForNetwork = modulesAllFile.exists() ? Channel.value(file(networkModulesAllPath)) : Channel.value(file(emptyModulesPath))
        }
    }
    def graph_network_stage = null
    if( networkEnabled ) {
        def networkMetadataChannel = metaMicroForNetwork != null ? metaMicroForNetwork : Channel.value(file(networkMetadataPath))
        def asvMagReadyForNetwork = asv_mag_link_stage != null ? asv_mag_link_stage.done : Channel.value(file(emptyModulesPath))
        graph_network_stage = RUN_GRAPH_NETWORK(
            graphAllForNetwork,
            graphThrForNetwork,
            nodeFeaturesForNetwork,
            asvFinalForNetwork,
            networkMetadataChannel,
            asvMagReadyForNetwork,
            taxonomy_stage.taxonomy_table,
            indicspeciesTablesForOverlay,
            modulesSubForNetwork,
            modulesAllForNetwork
        )
    }
    def asv_mag_network_stage = null
    if( asvMagNetworkEnabled ) {
        def asvMagReadyForAsvMagNetwork = asv_mag_link_stage != null ? asv_mag_link_stage.done : Channel.value(file(emptyModulesPath))
        def graphForAsvMagNetwork = asvMagNetworkGraphVariant == 'all' ? graphAllForAsvMagNetwork : graphThrForAsvMagNetwork
        asv_mag_network_stage = RUN_ASV_MAG_NETWORK(
            graphForAsvMagNetwork,
            nodeFeaturesForAsvMagNetwork,
            taxonomy_stage.taxonomy_table,
            metadata_analysis_stage.asv_final_spieceasi,
            asvMagReadyForAsvMagNetwork
        )
    }
    def module_mag_anchors_stage = null
    if( networkEnabled && asvMagLinkEnabled ) {
        def moduleMagGraphDone = graph_network_stage != null ? graph_network_stage.done : Channel.value(file(emptyModulesPath))
        def moduleMagAsvDone = asv_mag_link_stage != null ? asv_mag_link_stage.done : Channel.value(file(emptyModulesPath))
        module_mag_anchors_stage = RUN_MODULE_MAG_ANCHORS(
            modulesAllForNetwork,
            nodeFeaturesForNetwork,
            taxonomy_stage.taxonomy_table,
            metadata_analysis_stage.asv_final_spieceasi,
            metadata_analysis_stage.meta_micro_network,
            moduleMagAsvDone,
            moduleMagGraphDone
        )
    }
    def sankey_stage = null
    if( sankeyEnabled ) {
        sankey_stage = SANKEY(
            general_stats_stage.fastq_stats,
            general_stats_stage.filtered_stats,
            asv_counts_for_sankey,
            filter_counts_stage.filtered_decon,
            filter_counts_stage.filtered_micro
        )
    }
    if( masterSummaryEnabled ) {
        if( asvMetaForMasterSummary == null || asvFinalForMasterSummary == null ) {
            exit 1, "master_summary.enabled requires ASV_meta and ASV_final inputs from metadata/batch stages"
        }
        def masterSummaryNetworkDone = graph_network_stage != null ? graph_network_stage.done : Channel.value(file(emptyModulesPath))
        def masterSummarySankeyDone = sankey_stage != null ? sankey_stage.done : Channel.value(file(emptyModulesPath))
        def masterSummaryAsvMagDone = asv_mag_link_stage != null ? asv_mag_link_stage.done : Channel.value(file(emptyModulesPath))
        def masterSummaryOptionalDone = Channel.value(file(emptyModulesPath))
        MASTER_SUMMARY(
            asvMetaForMasterSummary,
            asvFinalForMasterSummary,
            masterSummaryNetworkDone,
            masterSummarySankeyDone,
            masterSummaryAsvMagDone,
            masterSummaryOptionalDone
        )
    }
}

workflow RUN_METADATA_ANALYSES {
    take:
    fastq_stats
    filtered_micro
    filtered_mito
    taxonomy_table

    main:
    metadata_stage = PLOT_METADATA(
        fastq_stats,
        filtered_micro,
        filtered_mito,
        taxonomy_table
    )

    metaMicroForBatch = metadata_stage.metadata_micro.map { it }
    metaMicroForOutlier = metadata_stage.metadata_micro.map { it }
    metaMicroForPlotUpset = metadata_stage.metadata_micro.map { it }
    metaMicroForCollectors = metadata_stage.metadata_micro.map { it }
    metaMicroForDiversity = metadata_stage.metadata_micro.map { it }
    metaMicroForIndicspecies = metadata_stage.metadata_micro.map { it }
    metaMicroForIndicspeciesPlots = metadata_stage.metadata_micro.map { it }
    metaMicroForClustermaps = metadata_stage.metadata_micro.map { it }
    metaMicroForNetwork = metadata_stage.metadata_micro.map { it }
    metaMicroForMeasurementAssociation = metadata_stage.metadata_micro.map { it }
    metaMicroForGroupingDiagnostics = metadata_stage.metadata_micro.map { it }
    asvMetaForBatch = metadata_stage.asv_meta_micro.map { it }
    asvMetaForGroupAugmentation = metadata_stage.asv_meta_micro.map { it }
    asvMetaSeedForCorrection = metadata_stage.asv_meta_micro.map { it }
    asvMetaForBubbleplotter = metadata_stage.asv_meta_micro.map { it }
    asvMetaForUmap = metadata_stage.asv_meta_micro.map { it }
    asvMetaForClustermaps = metadata_stage.asv_meta_micro.map { it }
    asvMetaForVocCorrelation = metadata_stage.asv_meta_micro.map { it }
    asvMetaForMeasurementAssociation = metadata_stage.asv_meta_micro.map { it }
    asvMetaForPowerAnalysis = metadata_stage.asv_meta_micro.map { it }
    asvMetaForTaxonomyPatientAware = metadata_stage.asv_meta_micro.map { it }
    asvMetaForLungStatus = metadata_stage.asv_meta_micro.map { it }
    asvMetaForMasterSummary = metadata_stage.asv_meta_micro.map { it }

    asvFinalForBatch = metadata_stage.asv_final_micro.map { it }
    asvFinalForCollectors = metadata_stage.asv_final_micro.map { it }
    asvFinalForDiversity = metadata_stage.asv_final_micro.map { it }
    asvFinalForIndicspecies = metadata_stage.asv_final_micro.map { it }
    asvFinalForSpieceasi = metadata_stage.asv_final_micro.map { it }
    asvFinalForNetwork = metadata_stage.asv_final_micro.map { it }
    asvFinalForVocCorrelation = metadata_stage.asv_final_micro.map { it }
    asvFinalForMeasurementAssociation = metadata_stage.asv_final_micro.map { it }
    asvFinalForGroupingDiagnostics = metadata_stage.asv_final_micro.map { it }
    asvFinalForPowerAnalysis = metadata_stage.asv_final_micro.map { it }
    asvFinalForTaxonomyPatientAware = metadata_stage.asv_final_micro.map { it }
    asvFinalForLungStatus = metadata_stage.asv_final_micro.map { it }
    asvFinalForMasterSummary = metadata_stage.asv_final_micro.map { it }

    grouping_diagnostics_stage = null
    if( groupingDiagnosticsEnabled ) {
        grouping_diagnostics_stage = GROUPING_DIAGNOSTICS(
            metaMicroForGroupingDiagnostics,
            asvFinalForGroupingDiagnostics
        )
    }

    if( groupingDiagnosticsApplySoftLabels ) {
        group_label_augmentation_stage = GROUP_LABEL_AUGMENTATION(
            metadata_stage.metadata_micro.map { it },
            asvMetaForGroupAugmentation,
            grouping_diagnostics_stage.soft_assignments,
            grouping_diagnostics_stage.soft_validation_summary
        )
        metaMicroForBatch = group_label_augmentation_stage.metadata_augmented.map { it }
        metaMicroForOutlier = group_label_augmentation_stage.metadata_augmented.map { it }
        metaMicroForPlotUpset = group_label_augmentation_stage.metadata_augmented.map { it }
        metaMicroForCollectors = group_label_augmentation_stage.metadata_augmented.map { it }
        metaMicroForDiversity = group_label_augmentation_stage.metadata_augmented.map { it }
        metaMicroForIndicspecies = group_label_augmentation_stage.metadata_augmented.map { it }
        metaMicroForIndicspeciesPlots = group_label_augmentation_stage.metadata_augmented.map { it }
        metaMicroForClustermaps = group_label_augmentation_stage.metadata_augmented.map { it }
        metaMicroForNetwork = group_label_augmentation_stage.metadata_augmented.map { it }
        metaMicroForMeasurementAssociation = group_label_augmentation_stage.metadata_augmented.map { it }
        asvMetaForBatch = group_label_augmentation_stage.asv_meta_augmented.map { it }
        asvMetaSeedForCorrection = group_label_augmentation_stage.asv_meta_augmented.map { it }
        asvMetaForBubbleplotter = group_label_augmentation_stage.asv_meta_augmented.map { it }
        asvMetaForUmap = group_label_augmentation_stage.asv_meta_augmented.map { it }
        asvMetaForClustermaps = group_label_augmentation_stage.asv_meta_augmented.map { it }
        asvMetaForVocCorrelation = group_label_augmentation_stage.asv_meta_augmented.map { it }
        asvMetaForMeasurementAssociation = group_label_augmentation_stage.asv_meta_augmented.map { it }
        asvMetaForPowerAnalysis = group_label_augmentation_stage.asv_meta_augmented.map { it }
        asvMetaForTaxonomyPatientAware = group_label_augmentation_stage.asv_meta_augmented.map { it }
        asvMetaForLungStatus = group_label_augmentation_stage.asv_meta_augmented.map { it }
        asvMetaForMasterSummary = group_label_augmentation_stage.asv_meta_augmented.map { it }
    }

    if( plotUpsetEnabled ) {
        PLOT_UPSET(metaMicroForPlotUpset)
    }

    batch_stage = null
    asvClrForOutlier = null
    if( batchCorrectionEnabled ) {
        batch_stage = ASV_BATCH_CORRECTION(
            metaMicroForBatch,
            asvMetaForBatch,
            asvFinalForBatch
        )
        asvClrForOutlier = batch_stage.asv_clr_selected
        asvFinalForCollectors = batch_stage.asv_selected_counts_int.map { it }
        asvFinalForDiversity = batch_stage.asv_selected_counts_int.map { it }
        asvFinalForIndicspecies = batch_stage.asv_selected_counts_int.map { it }
        asvFinalForSpieceasi = batch_stage.asv_selected_counts_int.map { it }
        asvFinalForNetwork = batch_stage.asv_selected_counts_int.map { it }
        asvFinalForVocCorrelation = batch_stage.asv_selected_counts_int.map { it }
        asvFinalForMeasurementAssociation = batch_stage.asv_selected_counts_int.map { it }
        asvFinalForGroupingDiagnostics = batch_stage.asv_selected_counts_int.map { it }
        asvFinalForPowerAnalysis = batch_stage.asv_selected_counts_int.map { it }
        asvFinalForTaxonomyPatientAware = batch_stage.asv_selected_counts_int.map { it }
        asvFinalForLungStatus = batch_stage.asv_selected_counts_int.map { it }
        asvFinalForMasterSummary = batch_stage.asv_selected_counts_int.map { it }
        umapResultsForTrajectory = batch_stage.umap_results
        if( bubbleplotterEnabled || umapClusteringEnabled || clustermapsEnabled || vocCorrelationEnabled || measurementAssociationEnabled || masterSummaryEnabled || powerAnalysisEnabled || taxonomyPatientAwareEnabled || lungStatusAnalysisEnabled ) {
            corrected_asv_meta_stage = ASV_META_FROM_CORRECTED(
                asvMetaSeedForCorrection,
                batch_stage.asv_selected_counts_int
            )
            asvMetaForBubbleplotter = corrected_asv_meta_stage.asv_meta_corrected.map { it }
            asvMetaForUmap = corrected_asv_meta_stage.asv_meta_corrected.map { it }
            asvMetaForClustermaps = corrected_asv_meta_stage.asv_meta_corrected.map { it }
            asvMetaForVocCorrelation = corrected_asv_meta_stage.asv_meta_corrected.map { it }
            asvMetaForMeasurementAssociation = corrected_asv_meta_stage.asv_meta_corrected.map { it }
            asvMetaForPowerAnalysis = corrected_asv_meta_stage.asv_meta_corrected.map { it }
            asvMetaForTaxonomyPatientAware = corrected_asv_meta_stage.asv_meta_corrected.map { it }
            asvMetaForLungStatus = corrected_asv_meta_stage.asv_meta_corrected.map { it }
            asvMetaForMasterSummary = corrected_asv_meta_stage.asv_meta_corrected.map { it }
        }
    }
    if( bubbleplotterEnabled ) {
        BUBBLEPLOTTER(asvMetaForBubbleplotter)
    }
    if( umapClusteringEnabled ) {
        UMAP_CLUSTERING(asvMetaForUmap)
    }
    if( outlierEnabled ) {
        OUTLIER_CHECKER(
            asvClrForOutlier,
            metaMicroForOutlier
        )
    }
    if( collectorsEnabled ) {
        COLLECTORS_CURVE(
            asvFinalForCollectors,
            metaMicroForCollectors
        )
    }
    if( diversityEnabled ) {
        DIVERSITY_ANALYSIS(
            metaMicroForDiversity,
            asvFinalForDiversity
        )
    }

    indicspecies_stage = null
    if( indicspeciesEnabled ) {
        indicspecies_stage = INDICSPECIES(
            metaMicroForIndicspecies,
            asvFinalForIndicspecies
        )
        if( indicspeciesPlotEnabled ) {
            INDICSPECIES_PLOTS(
                metaMicroForIndicspeciesPlots,
                indicspecies_stage.all_tables.collect()
            )
        }
        if( indicspeciesAlignedEnabled ) {
            INDICSPECIES_ALIGNED_PLOTS(
                indicspecies_stage.all_tables.collect()
            )
        }
    }

    indicspeciesReadyForClustermaps = indicspeciesEnabled ? indicspecies_stage.done.map { true } : Channel.value(false)
    indicspeciesReadyForPowerAnalysis = indicspeciesEnabled ? indicspecies_stage.done.map { true } : Channel.value(false)
    indicspeciesTablesForOverlay = indicspeciesEnabled ? indicspecies_stage.all_tables.collect() : Channel.value(file(emptyModulesPath))
    indicspeciesTablesForVocCorrelation = indicspeciesEnabled ? indicspecies_stage.all_tables.collect() : Channel.value(file(emptyModulesPath))

    if( vocCorrelationEnabled ) {
        VOC_CORRELATION(
            asvMetaForVocCorrelation,
            asvFinalForVocCorrelation,
            indicspeciesTablesForVocCorrelation
        )
    }

    if( measurementAssociationEnabled ) {
        MEASUREMENT_ASSOCIATION(
            asvMetaForMeasurementAssociation,
            metaMicroForMeasurementAssociation,
            asvFinalForMeasurementAssociation
        )
    }

    if( clustermapsEnabled ) {
        CLUSTERMAPS(
            asvMetaForClustermaps,
            metaMicroForClustermaps,
            indicspeciesReadyForClustermaps
        )
    }
    if( powerAnalysisEnabled ) {
        GROUP_POWER_ANALYSIS(
            asvMetaForPowerAnalysis,
            asvFinalForPowerAnalysis,
            indicspeciesReadyForPowerAnalysis
        )
    }
    if( taxonomyPatientAwareEnabled ) {
        TAXONOMY_GROUP_ASSOCIATION(
            asvMetaForTaxonomyPatientAware,
            asvFinalForTaxonomyPatientAware
        )
    }
    if( lungStatusAnalysisEnabled ) {
        PAIRED_GROUP_CONTRAST(
            asvMetaForLungStatus,
            asvFinalForLungStatus
        )
    }

    emit:
    meta_micro_network = metaMicroForNetwork
    asv_final_spieceasi = asvFinalForSpieceasi
    asv_final_network = asvFinalForNetwork
    asv_meta_master_summary = asvMetaForMasterSummary
    asv_final_master_summary = asvFinalForMasterSummary
    indicspecies_tables = indicspeciesTablesForOverlay
    indicspecies_group1_summary = indicspeciesEnabled ? indicspecies_stage.group1_summary : Channel.value(file(emptyModulesPath))
}

workflow RUN_GRAPH_NETWORK {
    take:
    graph_all
    graph_thr
    node_features
    asv_final
    network_metadata
    asv_mag_done
    taxonomy_table
    isa_tables
    modules_sub
    modules_all

    main:
    stage = GRAPH_NETWORK(
        graph_all,
        graph_thr,
        node_features,
        asv_final,
        network_metadata,
        asv_mag_done,
        taxonomy_table,
        isa_tables,
        modules_sub,
        modules_all
    )

    emit:
    done = stage.done
}

workflow RUN_ASV_MAG_NETWORK {
    take:
    graph
    node_features
    taxonomy_table
    asv_counts
    dep_asv_mag

    main:
    stage = ASV_MAG_NETWORK(
        graph,
        node_features,
        taxonomy_table,
        asv_counts,
        dep_asv_mag
    )

    emit:
    done = stage.done
}

workflow RUN_MODULE_MAG_ANCHORS {
    take:
    modules_all
    node_features
    taxonomy_table
    asv_counts
    metadata_table
    dep_asv_mag
    dep_graph_network

    main:
    stage = MODULE_MAG_ANCHORS(
        modules_all,
        node_features,
        taxonomy_table,
        asv_counts,
        metadata_table,
        dep_asv_mag,
        dep_graph_network
    )

    emit:
    asv_anchor_table = stage.asv_anchor_table
    module_summary = stage.module_summary
    sample_module_scores = stage.sample_module_scores
    sample_top_modules = stage.sample_top_modules
    sample_module_matrix = stage.sample_module_matrix
    done = stage.done
}

process FASTP_QC {
    tag { meta.sample_id }
    cpus sampleThreads
    conda "${condaEnvPath}"
    publishDir dirMap.fastp, mode: 'copy', pattern: '*', saveAs: { filename ->
        if( filename == 'R1.fastq.gz' ) {
            return "${meta.sample_id}_R1.fastq.gz"
        }
        if( filename == 'R2.fastq.gz' ) {
            return "${meta.sample_id}_R2.fastq.gz"
        }
        if( filename == 'fastp.json' ) {
            return "${meta.sample_id}.fastp.json"
        }
        if( filename == 'fastp.html' ) {
            return "${meta.sample_id}.fastp.html"
        }
        return filename
    }

    input:
    tuple val(meta), path(r1), path(r2)

    output:
    tuple val(meta), path("R1.fastq.gz"), path("R2.fastq.gz"), emit: reads
    path("fastp.json"), emit: fastp_json
    path("fastp.html"), emit: fastp_html

    script:
    def trimFrontR1 = fastpTrimValues.front_r1
    def trimTailR1  = fastpTrimValues.tail_r1
    def trimFrontR2 = fastpTrimValues.front_r2
    def trimTailR2  = fastpTrimValues.tail_r2
    if( meta.paired && r2 ) {
        return """
fastp \\
  -i "${r1}" -I "${r2}" \\
  -o R1.fastq.gz \\
  -O R2.fastq.gz \\
  -f ${trimFrontR1} -t ${trimTailR1} \\
  -F ${trimFrontR2} -T ${trimTailR2} \\
  -j fastp.json \\
  -h fastp.html \\
  -w ${task.cpus}
"""
    }
    return """
fastp \\
  -i "${r1}" \\
  -o R1.fastq.gz \\
  -f ${trimFrontR1} -t ${trimTailR1} \\
  -j fastp.json \\
  -h fastp.html \\
  -w ${task.cpus}
\nln -sf R1.fastq.gz R2.fastq.gz\n
"""
}

process MERGE_READS {
    tag { meta.sample_id }
    cpus sampleThreads
    conda "${condaEnvPath}"
    publishDir dirMap.merge, mode: 'copy', saveAs: { filename ->
        filename == 'merged.fastq.gz' ? "${meta.sample_id}.merged.fastq.gz" : filename
    }

    input:
    tuple val(meta), path(r1), path(r2)

    output:
    tuple val(meta), path("merged.fastq.gz")

    script:
    def allowStagger = mergeAllowStagger ? '--fastq_allowmergestagger' : ''
    if( meta.paired && r2 ) {
        """
set -euo pipefail
vsearch --fastq_mergepairs "${r1}" \\
        --reverse "${r2}" \\
        --fastqout merged.fastq \\
        --fastq_maxdiffs ${mergeMaxDiffs} \\
        --fastq_minovlen ${mergeMinOverlap} \\
        --fastq_truncqual ${mergeTruncQuality} \\
        ${allowStagger} \\
        --threads ${task.cpus}
gzip -n merged.fastq
"""
    } else {
        """
set -euo pipefail
if [[ "${r1}" == *.gz ]]; then
  gunzip -c "${r1}" > merged.fastq
else
  cat "${r1}" > merged.fastq
fi
gzip -n merged.fastq
"""
    }
}

process FILTER_READS {
    tag { meta.sample_id }
    cpus sampleThreads
    conda "${condaEnvPath}"
    publishDir dirMap.filter, mode: 'copy', saveAs: { filename ->
        filename == 'filtered.fasta.gz' ? "${meta.sample_id}.filtered.fasta.gz" : filename
    }

    input:
    tuple val(meta), path(merged_fastq)

    output:
    tuple val(meta), path("filtered.fasta.gz")

    script:
    """
set -euo pipefail
gzip -cd "${merged_fastq}" > merged.fastq
vsearch --fastx_filter merged.fastq \\
        --fastq_maxee ${filterMaxEe} \\
        --fastq_minlen ${filterMinLen} \\
        --fastq_maxlen ${filterMaxLen} \\
        --fastaout filtered.fasta
gzip -n filtered.fasta
"""
}

process RELABEL_FILTERED {
    tag { meta.sample_id }
    cpus 1
    conda "${condaEnvPath}"
    publishDir dirMap.concat, mode: 'copy', pattern: '*.fasta.gz'

    input:
    tuple val(meta), path(filtered_fasta)

    output:
    tuple val(meta), path("*.filtered.relabel.fasta.gz"), emit: relabeled

    script:
    def labelSep = concatLabelSep
    def relabeledOut = "${meta.sample_id}.filtered.relabel.fasta.gz"
    """
awk -v pref="${meta.sample_id}" -v sep="${labelSep}" '{
  if (\$0 ~ /^>/) {
    sub(/^>[^:]*:/, ">" pref sep, \$0)
    if (\$0 !~ "^>" pref sep) \$0 = ">" pref sep substr(\$0, 2)
  }
  print
}' <(gzip -cd "${filtered_fasta}") | gzip -n > "${relabeledOut}"
"""
}

process CONCAT_FASTAS {
    cpus 1
    conda "${condaEnvPath}"
    publishDir dirMap.concat, mode: 'copy', pattern: '*.fasta.gz'

    input:
    path(filtered_fastas)

    output:
    path("concat.fasta.gz"), emit: concat_for_derep
    path("concat_counts.fasta.gz"), emit: concat_for_counts

    script:
    def concatInputs = filtered_fastas.collect { "\"${it}\"" }.join(' ')
    """
set -euo pipefail
for f in ${concatInputs}; do
  gzip -cd "\${f}" || cat "\${f}"
done > concat.fasta
gzip -n -c concat.fasta > concat.fasta.gz
cp concat.fasta.gz concat_counts.fasta.gz
"""
}

process DEREPLICATE {
    cpus pipelineThreads
    conda "${condaEnvPath}"
    publishDir dirMap.derep, mode: 'copy', pattern: '*'

    input:
    path(concat_fasta)

    output:
    path("derep.fasta.gz")

    script:
    """
set -euo pipefail
gzip -cd "${concat_fasta}" > concat.fasta
vsearch --derep_fulllength concat.fasta \\
        --output derep.fasta \\
        --sizeout \\
        --threads ${task.cpus} \\
        --log "${dirMap.logs}/derep.log"
gzip -n derep.fasta
"""
}

process SINA_TRIM {
    cpus sinaThreads
    conda "${sinaCondaEnvPath}"
    publishDir dirMap.sina, mode: 'copy', pattern: '*'

    input:
    path(derep_fasta)

    output:
    path("derep_trimmed.fasta.gz"), emit: trimmed_fasta
    path("derep_SINA.fasta.gz")
    path("derep_SINA.log")
    path("derep_v_regions.tsv")

    script:
    def parseVerboseArg = sinaVerbose ? ' --verbose' : ''
    def trimTargetArg = sinaTrimTarget ? " -t \"${sinaTrimTarget}\"" : ''
    def keepGapsArg = sinaKeepGaps ? ' --keep-gaps' : ''
    """
set -euo pipefail
gzip -cd "${derep_fasta}" > derep_input.fasta
sina \\
    -i derep_input.fasta \\
    -o derep_SINA.fasta \\
    -r "${sinaReferencePath}" \\
    -v \\
    -p ${task.cpus} \\
    --log-file derep_SINA.log

python "${parseSinaScriptPath}" \\
  --log derep_SINA.log \\
  --output derep_v_regions.tsv${parseVerboseArg}

python "${trimSinaScriptPath}" \\
  -m derep_v_regions.tsv \\
  -f derep_SINA.fasta \\
  -r "${sinaRegionsArg}"${trimTargetArg} \\
  -o derep_trimmed.fasta \\
  --id-column "${sinaIdColumn}" \\
  --threads ${task.cpus} \\
  --batch-size ${sinaBatchSize}${keepGapsArg}
gzip -n derep_SINA.fasta
gzip -n derep_trimmed.fasta
"""
}
process DENOISE {
    cpus pipelineThreads
    conda "${condaEnvPath}"
    publishDir dirMap.denoise, mode: 'copy', pattern: '*'

    input:
    path(trimmed_fasta)

    output:
    path("centroids.fasta.gz")

    script:
    def unoiseCfg = config.unoise ?: [:]
    """
set -euo pipefail
gzip -cd "${trimmed_fasta}" > derep_trimmed.fasta
vsearch --cluster_unoise derep_trimmed.fasta \\
        --centroids centroids.fasta \\
        --sizein --sizeout --relabel ASV \\
        --minsize ${unoiseCfg.min_size ?: 3} \\
        --threads ${task.cpus} \\
        --log "${dirMap.logs}/denoise.log"
gzip -n centroids.fasta
"""
}

process CHIMERA_CHECK {
    cpus pipelineThreads
    conda "${condaEnvPath}"
    publishDir dirMap.nochi, mode: 'copy', pattern: '*'

    input:
    path(centroids)

    output:
    path("nochimeras.fasta.gz")

    script:
    """
set -euo pipefail
gzip -cd "${centroids}" > centroids.fasta
vsearch --uchime3_denovo centroids.fasta \\
        --nonchimeras nochimeras.fasta \\
        --sizein \\
        --threads ${task.cpus} \\
        --log "${dirMap.logs}/nochimera.log"
gzip -n nochimeras.fasta
"""
}

process CREATE_COUNT_MATRIX {
    cpus pipelineThreads
    conda "${condaEnvPath}"
    publishDir dirMap.asv, mode: 'copy', pattern: '*'

    input:
    path(concat_fasta)
    path(nochimeras)

    output:
    tuple path("ASV_counts.tsv"), path("ASVs.fasta.gz"), emit: count_matrix

    script:
    """
set -euo pipefail
gzip -cd "${nochimeras}" > ASVs.fasta
gzip -cd "${concat_fasta}" > concat_counts.fasta
vsearch --usearch_global concat_counts.fasta \\
        --db ASVs.fasta \\
        --id 0.999 \\
        --otutabout ASV_counts.tsv \\
        --threads ${task.cpus} \\
        --log "${dirMap.logs}/count.log"
gzip -n ASVs.fasta
"""
}

process FILTER_TABLE {
    conda "${condaEnvPath}"
    publishDir dirMap.asv, mode: 'copy', pattern: '*'

    input:
    tuple path(count_table), path(asv_fasta)

    output:
    tuple path("ASV_filtered.tsv"), path("ASVs_filtered.fasta.gz"), emit: filtered

    script:
    def tableCfg = config.table_filter ?: [:]
    """
set -euo pipefail
gzip -cd "${asv_fasta}" > ASVs.fasta
python "${tableScriptFile}" \\
       "${count_table}" \\
       ASV_filtered.tsv \\
       ${tableCfg.min_sample_sum ?: 5000} \\
       ${tableCfg.min_asv_sum ?: 0.01} \\
       ASVs.fasta \\
       ASVs_filtered.fasta
gzip -n ASVs_filtered.fasta
"""
}

process TAXONOMY {
    cpus taxonomyThreads
    conda "${taxonomyCondaEnvPath}"
    publishDir dirMap.taxonomy, mode: 'copy', pattern: '*'

    input:
    path(filtered_fasta)

    output:
    path("${taxonomyUppercaseGzName}")
    path("${taxonomyOutputName}"), emit: taxonomy_table
    path("${taxonomyStatsName}")

    script:
    """
set -euo pipefail
gzip -cd "${filtered_fasta}" > filtered_input.fasta
python - <<'PY' filtered_input.fasta '${taxonomyUppercasePlainName}'
import sys
from pathlib import Path
src = Path(sys.argv[1])
dst = Path(sys.argv[2])
with src.open() as inp, dst.open('w') as out:
    for line in inp:
        if line.startswith('>'):
            out.write(line)
        else:
            out.write(line.strip().upper() + '\\n')
PY

python "${taxonomyScriptPath}" \\
  --input-fasta "${taxonomyUppercasePlainName}" \\
  --ref-taxonomy "${taxonomyRefTaxonomy}" \\
  --ref-seqs "${taxonomyRefSequences}" \\
  --output-tsv "${taxonomyOutputName}" \\
  --stats-output "${taxonomyStatsName}" \\
  --threads ${task.cpus}
gzip -n -c "${taxonomyUppercasePlainName}" > "${taxonomyUppercaseGzName}"
"""
}

process PREPARE_BLAST_DATABASES {
    cpus 1
    conda "${mitomasterCondaEnvPath}"
    publishDir dirMap.reference, mode: 'copy', pattern: 'blast_databases'

    input:
    val(mito_source)
    val(contaminant_source)

    output:
    tuple path("blast_databases/mitochondrial"), path("blast_databases/contaminants"), emit: databases

    script:
    def mitoSourceCommand = mitoBlastFastaPath ?
        (mitoBlastFastaPath.toLowerCase().endsWith('.gz') ?
            "gzip -cd \"${mitoBlastFastaPath}\" > blast_databases/mitochondrial/source.fasta" :
            "cp \"${mitoBlastFastaPath}\" blast_databases/mitochondrial/source.fasta") :
        "blastdbcmd -db \"${mitoBlastDbPath}\" -entry all -out blast_databases/mitochondrial/source.fasta"
    def contaminantSourceCommand = mitoBiofFastaPath ?
        (mitoBiofFastaPath.toLowerCase().endsWith('.gz') ?
            "gzip -cd \"${mitoBiofFastaPath}\" > blast_databases/contaminants/source.fasta" :
            "cp \"${mitoBiofFastaPath}\" blast_databases/contaminants/source.fasta") :
        "blastdbcmd -db \"${mitoBiofDbPath}\" -entry all -out blast_databases/contaminants/source.fasta"
    """
set -euo pipefail
mkdir -p blast_databases/mitochondrial blast_databases/contaminants
${mitoSourceCommand}
${contaminantSourceCommand}
makeblastdb -in blast_databases/mitochondrial/source.fasta -dbtype nucl -parse_seqids -out blast_databases/mitochondrial/db
makeblastdb -in blast_databases/contaminants/source.fasta -dbtype nucl -parse_seqids -out blast_databases/contaminants/db
"""
}

process MITOMASTER {
    cpus mitoBlastThreads
    conda "${mitomasterCondaEnvPath}"
    publishDir mitoOutputDirPath, mode: 'copy', pattern: '*'

    input:
    tuple path(filtered_table), path(filtered_fasta)
    tuple path(mito_db_dir), path(contaminant_db_dir)

    output:
    tuple path("mitomaster_output.tsv"), path("mito_ncbi.blast6.tsv"), path("ssu_pipeline_contaminants.blast6.tsv"), emit: mito_artifacts

    script:
    def filteredFastaPath = filtered_fasta.toString().trim()
    def mitomasterCommand = mitoRunMitomaster ? """
python "${mitomasterScriptPath}" \\
  --data-dir "${mitoChunkDirPath}" \\
  --glob-pattern "*.fa*" \\
  --recursive \\
  --output-file mitomaster_output.tsv \\
  --max-workers ${mitomasterWorkers} \\
  --timeout ${mitomasterTimeout} \\
  --retries ${mitomasterRetries} \\
  --header-mode "${mitomasterHeaderMode}" \\
  --overwrite
""" : "printf 'Sequence_ID\\thaplo\\n' > mitomaster_output.tsv"
    """
set -euo pipefail
rm -rf "${mitoChunkDirPath}"
mkdir -p "${mitoChunkDirPath}"
gzip -cd "${filteredFastaPath}" > filtered_input.fasta
FILTERED_FASTA=\$(realpath filtered_input.fasta)

seqkit split -s ${mitoChunkSize} -O "${mitoChunkDirPath}" "\${FILTERED_FASTA}"

${mitomasterCommand}

blastn -query "\${FILTERED_FASTA}" \\
  -db "${mito_db_dir}/db" \\
  -outfmt "6 qseqid sseqid pident length qlen mismatch gapopen qstart qend sstart send evalue bitscore" \\
  -out mito_ncbi.blast6.tsv \\
  -num_threads ${task.cpus}

blastn -query "\${FILTERED_FASTA}" \\
  -db "${contaminant_db_dir}/db" \\
  -outfmt "6 qseqid sseqid pident length qlen mismatch gapopen qstart qend sstart send evalue bitscore" \\
  -out ssu_pipeline_contaminants.blast6.tsv \\
  -num_threads ${task.cpus}
"""
}

process MITO_DECONTAM {
    cpus mitoBlastThreads
    conda "${mitoCheckerCondaEnvPath}"
    publishDir mitoOutputDirPath, mode: 'copy', pattern: '*'

    input:
    tuple path(mitomaster_file), path(mito_blast), path(biof_blast)
    path(taxonomy_table)

    output:
    path("${mitoPrefix}.master.tsv"), emit: nontarget_table
    path("${mitoPrefix}.summary_*.tsv"), optional: true, emit: summary_tables
    path("${mitoPrefix}_*.svg"), optional: true, emit: svg_plots
    path("${mitoPrefix}_*.pdf"), optional: true, emit: pdf_plots
    path("${mitoPrefix}_*.png"), optional: true, emit: png_plots

    script:
    def noPlotsFlag = mitoNoPlots ? ' --no-plots' : ''
    """
python "${mitoCheckerScriptPath}" \\
  --mitomaster-file "${mitomaster_file}" \\
  --mito-blast "${mito_blast}" \\
  --silva-tax "${taxonomy_table}" \\
  --biof-file "${biof_blast}" \\
  --output-dir "." \\
  --prefix "${mitoPrefix}" \\
  --formats "${mitoFormats}" \\
  --min-pident ${mitoMinPident} \\
  --min-percov ${mitoMinPercov} \\
  --mitochondria-substring "${mitoMitoSubstring}" \\
  --feature-col "${mitoFeatureCol}" \\
  --taxon-col "${mitoTaxonCol}" \\
  --consensus-col "${mitoConsensusCol}" \\
  --steps "${mitoSteps}" \\
  --host-first-step "${mitoHostFirstStep}" \\
  --figsize "${mitoFigsize}" \\
  --style "${mitoStyle}" \\
  --dpi ${mitoDpi} \\
  --overwrite${noPlotsFlag}
"""
}

process FILTER_COUNTS {
    cpus pipelineThreads
    conda "${filterCountsCondaEnvPath}"
    publishDir dirMap.asv, mode: 'copy', pattern: '*', saveAs: { filename ->
        filename.endsWith('.mito.tsv') ? null : filename
    }
    publishDir filterCountsMitoDir, mode: 'copy', pattern: '*.mito.tsv'

    input:
    tuple path(count_table), path(asv_fasta)
    path(taxonomy_table)
    path(nontarget_table)

    output:
    path("${filterCountsOutputName}"), emit: filtered_counts
    path("${filterCountsOutputName}".replace('.tsv','.micro.tsv')), optional: true, emit: filtered_micro
    path("${filterCountsOutputName}".replace('.tsv','.mito.tsv')), emit: filtered_mito
    path("${filterCountsOutputName}".replace('.tsv','.decon.tsv')), optional: true, emit: filtered_decon

    when:
    filterCountsEnabled

    script:
    def metadataArg = filterCountsMetadataPath ? """  --metadata "${filterCountsMetadataPath}" \\\n""" : ''
    def groupArg = filterCountsGroupCol ? """  --group-col "${filterCountsGroupCol}" \\\n""" : ''
    def saveInterArg = filterCountsSaveIntermediates ? "  --save-intermediates \\\n" : ''
    def mitoColsArg = (filterCountsMitoCols && !filterCountsMitoCols.isEmpty()) ?
        """  --mito-cols ${filterCountsMitoCols.collect { "\"${it}\"" }.join(' ')} \\\n""" : ''
    def excludeTaxaArg = (filterCountsExcludeTaxa && !filterCountsExcludeTaxa.isEmpty()) ?
        filterCountsExcludeTaxa.collect { item -> """  --exclude-taxon "${item}" \\\n""" }.join('') : ''
"""
set -euo pipefail
echo "filter_nontarget.py md5: ${filterCountsScriptHash}"
python "${filterCountsScriptPath}" \\
  --count-table "${count_table}" \\
  --nontarget-table "${nontarget_table}" \\
  --taxonomy-table "${taxonomy_table}" \\
${metadataArg}${groupArg}  --min-group-size ${filterCountsMinGroup} \\
  --abundance-threshold ${filterCountsAbundance} \\
  --sample-id-col "${filterCountsSampleCol}" \\
  --min-consensus ${filterCountsMinConsensus} \\
  --taxon-col "${filterCountsTaxonCol}" \\
  --consensus-col "${filterCountsConsensusCol}" \\
  --biofactorial-col "${filterCountsBiofactorialCol}" \\
${mitoColsArg}${excludeTaxaArg}  --mito-output-dir "." \\
  --output "${filterCountsOutputName}" \\
${saveInterArg}
"""
}

process SANKEY {
    cpus 1
    conda "${sankeyCondaEnvPath}"

    input:
    path(fastq_stats)
    path(filtered_stats)
    path(asv_counts)
    path(asv_decon_counts)
    path(asv_micro_counts)

    output:
    path("sankey.done"), emit: done

    when:
    sankeyEnabled

    script:
    def keepTypesArg = sankeyKeepTypes && !sankeyKeepTypes.isEmpty() ? "  --keep-types \"${sankeyKeepTypes.join(',')}\" \\\n" : ''
    def verticalOrderArg = sankeyVerticalOrder && !sankeyVerticalOrder.isEmpty() ? "  --vertical-order \"${sankeyVerticalOrder.join(',')}\" \\\n" : ''
    def rawOutputPrefix = "${sankeyOutputPrefix}_raw"
    def labeledFlag = sankeyMakeLabeled ? "  --make-labeled \\\n" : ''
    def unlabeledFlag = sankeyMakeUnlabeled ? "  --make-unlabeled \\\n" : ''
"""
set -euo pipefail
echo "sankey_builder.py md5: ${sankeyScriptHash}"

python3 "${sankeyScriptPath}" \\
  --data-dir "${outputDir}" \\
  --sub-dir "${sankeySubDir}" \\
  --metadata "${sankeyMetadataPath}" \\
  --sample-manifest "${manifestPath}" \\
  --samp-col "${sankeySampCol}" \\
  --group1-col "${sankeyGroupCol}" \\
  --color-col "${sankeyColorCol}" \\
${keepTypesArg}${verticalOrderArg}  --fastq-stats stats/"${fastq_stats}" \\
  --filtered-stats stats/"${filtered_stats}" \\
  --asv-raw ASVs/"${asv_counts}" \\
  --asv-decon ASVs/"${asv_decon_counts}" \\
  --asv-micro ASVs/"${asv_micro_counts}" \\
  --title "${sankeyTitle}" \\
  --arrangement "${sankeyArrangement}" \\
  --output-prefix "${sankeyOutputPrefix}" \\
${labeledFlag}${unlabeledFlag}  --verbose

python3 "${sankeyScriptPath}" \\
  --data-dir "${outputDir}" \\
  --sub-dir "${sankeySubDir}" \\
  --metadata "${sankeyMetadataPath}" \\
  --sample-manifest "${manifestPath}" \\
  --samp-col "${sankeySampCol}" \\
  --group1-col "${sankeyGroupCol}" \\
  --color-col "${sankeyColorCol}" \\
${verticalOrderArg}  --fastq-stats stats/"${fastq_stats}" \\
  --filtered-stats stats/"${filtered_stats}" \\
  --asv-raw ASVs/"${asv_counts}" \\
  --asv-decon ASVs/"${asv_decon_counts}" \\
  --asv-micro ASVs/"${asv_micro_counts}" \\
  --title "${sankeyTitle}" \\
  --arrangement "${sankeyArrangement}" \\
  --output-prefix "${rawOutputPrefix}" \\
${labeledFlag}${unlabeledFlag}  --all-samples \\
  --verbose

touch sankey.done
"""
}

process GENERAL_STATS {
    cpus pipelineThreads
    conda "${generalStatsCondaEnvPath}"
    publishDir dirMap.stats, mode: 'copy', pattern: '*'

    input:
    path(concat_fasta)

    output:
    path("fastq_stats.tsv"), emit: fastq_stats
    path("fastp_fastqs.tsv"), emit: fastp_stats
    path("filtered_fastas.tsv"), emit: filtered_stats
    path("concat_fastas.tsv"), emit: concat_stats

    script:
    def rawArgs = generalStatsRawArgs
    def fastpArgs = generalStatsFastpArgs
    def filteredArgs = generalStatsFilteredArgs
    """
set -euo pipefail

run_seqkit() {
  local outfile="\$1"
  shift
  if [[ "\$#" -eq 0 ]]; then
    : > "\${outfile}"
    return
  fi
  seqkit stat -a -T -j ${task.cpus} -o "\${outfile}" "\$@"
}

run_seqkit fastq_stats.tsv ${rawArgs}
run_seqkit fastp_fastqs.tsv ${fastpArgs}
run_seqkit filtered_fastas.tsv ${filteredArgs}
run_seqkit concat_fastas.tsv "${concat_fasta}"
"""
}

process PLOT_METADATA {
    cpus pipelineThreads
    conda "${plotMetadataCondaEnvPath}"

    input:
    path(fastq_stats)
    path(asv_micro)
    path(asv_mito)
    path(taxonomy_table)

    output:
    path("metadata_updated_micro.tsv"), emit: metadata_micro
    path("ASV_meta_micro.tsv"), emit: asv_meta_micro
    path("ASV_final.micro.tsv"), emit: asv_final_micro
    path("metadata_updated_mito.tsv"), optional: true, emit: metadata_mito
    path("ASV_meta_mito.tsv"), optional: true, emit: asv_meta_mito
    path("ASV_final.mito.tsv"), optional: true, emit: asv_final_mito

    when:
    metadataPlotsEnabled

    script:
    def includeRankAppend = metadataIncludeRank && !metadataIncludeRank.isEmpty() ?
        metadataIncludeRank.collect { "cmd+=( --include-rank \"${it}\" )" }.join('\n') : ''
    def metadataBiochemTable = metadataPlotsBiochemAssignmentsPath ?: ''
    def metadataBiochemIncludeCsv = metadataPlotsBiochemIncludeCols && !metadataPlotsBiochemIncludeCols.isEmpty() ? metadataPlotsBiochemIncludeCols.join(',') : ''
    def metadataBiochemMetaJoinCsv = metadataPlotsBiochemMetaJoinCols && !metadataPlotsBiochemMetaJoinCols.isEmpty() ? metadataPlotsBiochemMetaJoinCols.join(',') : ''
    def metadataBiochemJoinCsv = metadataPlotsBiochemJoinCols && !metadataPlotsBiochemJoinCols.isEmpty() ? metadataPlotsBiochemJoinCols.join(',') : ''
    def metadataStratTable = metadataPlotsStratificationTimeseriesPath ?: ''
    def metadataStratIncludeCsv = metadataPlotsStratIncludeCols && !metadataPlotsStratIncludeCols.isEmpty() ? metadataPlotsStratIncludeCols.join(',') : ''
    def metadataKeepTypesCsv = metadataKeepTypes && !metadataKeepTypes.isEmpty() ? metadataKeepTypes.join(',') : ''
    def metadataSubtractionGroupsCsv = metadataPlotsSubtractionGroups && !metadataPlotsSubtractionGroups.isEmpty() ? metadataPlotsSubtractionGroups.join(',') : ''
    def metadataGroupOrderCsv = metadataPlotsGroupOrder && !metadataPlotsGroupOrder.isEmpty() ? metadataPlotsGroupOrder.join(',') : ''
    def metadataNormalizationColsCsv = metadataGroupNormalizationCols.join(',')
    def metadataMicroFile = "${outputDir}/metadata/metadata_updated_micro.tsv"
    def metadataMitoFile = "${outputDir}/mito/metadata/metadata_updated_mito.tsv"
    def asvMetaMicroFile = "${outputDir}/metadata/ASV_meta_micro.tsv"
    def asvMetaMitoFile = "${outputDir}/mito/metadata/ASV_meta_mito.tsv"
    def asvTargetMicroFile = "${outputDir}/ASVs/${filterCountsOutputName}"
    def asvTargetMitoFile = "${outputDir}/mito/ASVs/${filterCountsOutputName.replace('.tsv','.mito.tsv')}"
    def asvFinalMicroFile = "${outputDir}/ASVs/ASV_final.micro.tsv"
    def asvFinalMitoFile = "${outputDir}/mito/ASVs/ASV_final.mito.tsv"
    def asvTaxTable = "${outputDir}/taxonomy/ASV_SILVA_tax.full-length.vsearch.tsv"
"""
set -euo pipefail
echo "plot_metadata.py md5: ${plotMetadataScriptHash}"

cmd=(
  python "${plotMetadataScriptPath}"
  --data-dir "${outputDir}"
  --sub-dir "${metadataPlotsSubDir}"
  --metadata "${metadataPlotsMetadataPath}"
  --taxonomy "${asvTaxTable}"
  --asv-micro "${asv_micro}"
  --asv-mito "${asv_mito}"
  --sample-id-col "${metadataPlotsSampleCol}"
  --group1-col "${metadataPlotsTypeCol}"
  --color-col "${metadataPlotsColorCol}"
  --subtraction-group-col "${metadataPlotsSubtractionCol}"
  --sample-manifest "${manifestPath}"
  --make-micro
  --make-mito
  --verbose
)
cmd+=( --subtraction-groups "${metadataSubtractionGroupsCsv}" )
${includeRankAppend}
if [[ -n "${metadataKeepTypesCsv}" ]]; then
  cmd+=( --keep-types "${metadataKeepTypesCsv}" )
fi
if [[ -n "${metadataGroupOrderCsv}" ]]; then
  cmd+=( --group-order "${metadataGroupOrderCsv}" )
fi

if [[ -n "${metadataBiochemTable}" ]]; then
  if [[ -f "${metadataBiochemTable}" ]]; then
    cmd+=( --biochem-assignments "${metadataBiochemTable}" )
    cmd+=( --biochem-sample-col "${metadataPlotsBiochemSampleCol}" )
    if [[ -n "${metadataBiochemIncludeCsv}" ]]; then
      cmd+=( --biochem-include-cols "${metadataBiochemIncludeCsv}" )
    fi
    if [[ -n "${metadataBiochemMetaJoinCsv}" || -n "${metadataBiochemJoinCsv}" ]]; then
      if [[ -n "${metadataBiochemMetaJoinCsv}" && -n "${metadataBiochemJoinCsv}" ]]; then
        cmd+=( --biochem-meta-join-cols "${metadataBiochemMetaJoinCsv}" )
        cmd+=( --biochem-join-cols "${metadataBiochemJoinCsv}" )
      else
        echo "[w] Incomplete biochem join config; both metadata and biochem join col lists are required. Falling back to sample-id join." >&2
      fi
    fi
  else
    echo "[w] metadata biochem assignments table not found; skipping merge: ${metadataBiochemTable}" >&2
  fi
fi

if [[ -n "${metadataStratTable}" ]]; then
  if [[ -f "${metadataStratTable}" ]]; then
    cmd+=( --stratification-timeseries "${metadataStratTable}" )
    cmd+=( --stratification-meta-join-col "${metadataPlotsStratMetaJoinCol}" )
    cmd+=( --stratification-join-col "${metadataPlotsStratJoinCol}" )
    if [[ -n "${metadataStratIncludeCsv}" ]]; then
      cmd+=( --stratification-include-cols "${metadataStratIncludeCsv}" )
    fi
  else
    echo "[w] metadata stratification table not found; skipping merge: ${metadataStratTable}" >&2
  fi
fi

if [[ "${metadataGroupNormalizationEnabled}" == "true" ]]; then
  cmd+=( --normalize-group-cols "${metadataNormalizationColsCsv}" )
  cmd+=( --normalize-group-pattern '${metadataGroupNormalizationPattern}' )
  cmd+=( --normalize-group-replacement "${metadataGroupNormalizationReplacement}" )
  if [[ "${metadataGroupNormalizationPreserveSource}" == "true" ]]; then
    cmd+=( --preserve-normalized-source )
  fi
fi

"\${cmd[@]}"

link_if_exists() {
  local src="\$1"
  local dest="\$2"
  if [[ -f "\${src}" ]]; then
    ln -sf "\${src}" "\${dest}"
  fi
}

link_if_exists "${metadataMicroFile}" "metadata_updated_micro.tsv"
link_if_exists "${asvMetaMicroFile}" "ASV_meta_micro.tsv"
link_if_exists "${asvFinalMicroFile}" "ASV_final.micro.tsv"
link_if_exists "${metadataMitoFile}" "metadata_updated_mito.tsv"
link_if_exists "${asvMetaMitoFile}" "ASV_meta_mito.tsv"
link_if_exists "${asvFinalMitoFile}" "ASV_final.mito.tsv"
"""
}

process PLOT_UPSET {
    cpus pipelineThreads
    conda "${plotUpsetCondaEnvPath}"

    input:
    path(metadata_table)

    output:
    path("plot_upset.done"), emit: done

    when:
    plotUpsetEnabled

    script:
    def taxonomyArg = plotUpsetTaxonomyPath ? """  --taxonomy-path "${plotUpsetTaxonomyPath}" \\\n""" : ''
    def groupOrderArg = plotUpsetGroupOrder && !plotUpsetGroupOrder.isEmpty() ? """  --group-order "${plotUpsetGroupOrder.join(',')}" \\\n""" : ''
    def subsetGroupsArg = plotUpsetSubsetGroups && !plotUpsetSubsetGroups.isEmpty() ? """  --subset-groups "${plotUpsetSubsetGroups.join(',')}" \\\n""" : ''
    def groupPaletteArg = plotUpsetGroupPalette ? """  --group-palette "${plotUpsetGroupPalette}" \\\n""" : ''
    def skipVennArg = plotUpsetSkipVenn ? "  --skip-venn \\\n" : ''
    def rawOnlyArg = plotUpsetRawOnly ? "  --raw-only \\\n" : ''
    def finalOnlyArg = plotUpsetFinalOnly ? "  --final-only \\\n" : ''
    def rawMicroMetadataPath = "${outputDir}/metadata/metadata_updated_micro_raw.tsv"
    def rawMicroFinalAsvPath = "${outputDir}/ASVs/ASV_final_raw.micro.tsv"
    def rawMicroAsvTargetPath = "${outputDir}/ASVs/ASV_target.micro.tsv"
    def rawMitoMetadataPath = "${outputDir}/mito/metadata/metadata_updated_mito_raw.tsv"
    def rawMitoFinalAsvPath = "${outputDir}/mito/ASVs/ASV_final_raw.mito.tsv"
    def rawMitoAsvTargetPath = "${outputDir}/mito/ASVs/ASV_target.mito.tsv"
    def rawMetadataPathSingle = plotUpsetDomain == 'mito' ? rawMitoMetadataPath : rawMicroMetadataPath
    def rawFinalAsvPathSingle = plotUpsetDomain == 'mito' ? rawMitoFinalAsvPath : rawMicroFinalAsvPath
    def rawAsvTargetPathSingle = plotUpsetDomain == 'mito' ? rawMitoAsvTargetPath : rawMicroAsvTargetPath
    """
set -euo pipefail

python "${plotUpsetScriptPath}" \\
  --data-dir "${outputDir}" \\
  --subdir "${plotUpsetSubDir}" \\
  --domain "${plotUpsetDomain}" \\
${taxonomyArg}  --sample-id-col "${plotUpsetSampleIdCol}" \\
  --group-col "${plotUpsetGroupCol}" \\
  --color-col "${plotUpsetColorCol}" \\
${groupPaletteArg}${groupOrderArg}${subsetGroupsArg}${skipVennArg}${rawOnlyArg}${finalOnlyArg}  --formats "${plotUpsetFormats}" \\
  --font-size ${plotUpsetFontSize}

if [[ "${plotUpsetDomain}" == "both" ]]; then
  python "${plotUpsetScriptPath}" \\
    --data-dir "${outputDir}" \\
    --subdir "${plotUpsetSubDir}" \\
    --domain "micro" \\
${taxonomyArg}    --sample-id-col "${plotUpsetSampleIdCol}" \\
    --group-col "${plotUpsetGroupCol}" \\
    --color-col "${plotUpsetColorCol}" \\
${groupPaletteArg}${groupOrderArg}${skipVennArg}${rawOnlyArg}${finalOnlyArg}    --formats "${plotUpsetFormats}" \\
    --font-size ${plotUpsetFontSize} \\
    --metadata-path "${rawMicroMetadataPath}" \\
    --asv-raw-path "${rawMicroAsvTargetPath}" \\
    --asv-final-path "${rawMicroFinalAsvPath}" \\
    --output-tag raw

  python "${plotUpsetScriptPath}" \\
    --data-dir "${outputDir}" \\
    --subdir "${plotUpsetSubDir}" \\
    --domain "mito" \\
${taxonomyArg}    --sample-id-col "${plotUpsetSampleIdCol}" \\
    --group-col "${plotUpsetGroupCol}" \\
    --color-col "${plotUpsetColorCol}" \\
${groupPaletteArg}${groupOrderArg}${skipVennArg}${rawOnlyArg}${finalOnlyArg}    --formats "${plotUpsetFormats}" \\
    --font-size ${plotUpsetFontSize} \\
    --metadata-path "${rawMitoMetadataPath}" \\
    --asv-raw-path "${rawMitoAsvTargetPath}" \\
    --asv-final-path "${rawMitoFinalAsvPath}" \\
    --output-tag raw
else
  python "${plotUpsetScriptPath}" \\
    --data-dir "${outputDir}" \\
    --subdir "${plotUpsetSubDir}" \\
    --domain "${plotUpsetDomain}" \\
${taxonomyArg}    --sample-id-col "${plotUpsetSampleIdCol}" \\
    --group-col "${plotUpsetGroupCol}" \\
    --color-col "${plotUpsetColorCol}" \\
${groupPaletteArg}${groupOrderArg}${skipVennArg}${rawOnlyArg}${finalOnlyArg}    --formats "${plotUpsetFormats}" \\
    --font-size ${plotUpsetFontSize} \\
    --metadata-path "${rawMetadataPathSingle}" \\
    --asv-raw-path "${rawAsvTargetPathSingle}" \\
    --asv-final-path "${rawFinalAsvPathSingle}" \\
    --output-tag raw
fi

touch plot_upset.done
"""
}

process BUBBLEPLOTTER {
    cpus pipelineThreads
    conda "${bubbleplotterCondaEnvPath}"

    input:
    path(asv_meta)

    output:
    path("bubbleplotter.done"), emit: done

    when:
    bubbleplotterEnabled

    script:
    def noAutoSizeArg = bubbleplotterNoAutoSize ? "  --no-auto-size \\\n" : ''
    def bubbleplotterGroup1OrderArg = bubbleplotterGroup1Order && !bubbleplotterGroup1Order.isEmpty() ? """  --group1-order "${bubbleplotterGroup1Order.join(',')}" \\\n""" : ''
    def bubbleplotterGroup2OrderArg = bubbleplotterGroup2Order && !bubbleplotterGroup2Order.isEmpty() ? """  --group2-order "${bubbleplotterGroup2Order.join(',')}" \\\n""" : ''
    """
set -euo pipefail
mkdir -p "${bubbleplotterOutputDirAbs}"

python "${bubbleplotterScriptPath}" \\
  --input "${asv_meta}" \\
  --output-prefix "${bubbleplotterOutputPrefixAbs}" \\
  --count-col "${bubbleplotterCountCol}" \\
  --sample-col "${bubbleplotterSampleCol}" \\
  --group1-col "${bubbleplotterDepthCol}" \\
  --color-col "${bubbleplotterColorCol}" \\
  --group2-col "${bubbleplotterMonthCol}" \\
${bubbleplotterGroup1OrderArg}${bubbleplotterGroup2OrderArg}${noAutoSizeArg}  --formats "${bubbleplotterFormats}" \\
  --figsize "${bubbleplotterFigsize}" \\
  --bubble-scale ${bubbleplotterScale}

touch bubbleplotter.done
"""
}

process UMAP_CLUSTERING {
    cpus pipelineThreads
    conda "${umapClusteringCondaEnvPath}"

    input:
    path(asv_meta)

    output:
    path("umap_clustering.done"), emit: done

    when:
    umapClusteringEnabled

    script:
    def umapGroup1OrderArg = umapClusteringGroup1Order && !umapClusteringGroup1Order.isEmpty() ? """  --group1-order "${umapClusteringGroup1Order.join(',')}" \\\n""" : ''
    def umapGroup2OrderArg = umapClusteringGroup2Order && !umapClusteringGroup2Order.isEmpty() ? """  --group2-order "${umapClusteringGroup2Order.join(',')}" \\\n""" : ''
    def umapNoScaleArg = umapClusteringNoScale ? "  --no-scale \\\n" : ''
    """
set -euo pipefail
mkdir -p "${umapClusteringOutputDirAbs}"

python "${umapClusteringScriptPath}" \\
  --input "${asv_meta}" \\
  --output-prefix "${umapClusteringOutputPrefixAbs}" \\
  --count-col "${umapClusteringCountCol}" \\
  --sample-col "${umapClusteringSampleCol}" \\
  --group1-col "${umapClusteringDepthCol}" \\
  --color-col "${umapClusteringColorCol}" \\
  --group2-col "${umapClusteringSecondaryCol}" \\
  --group1-palette "${umapClusteringGroup1Palette}" \\
  --group2-palette "${umapClusteringGroup2Palette}" \\
${umapGroup1OrderArg}${umapGroup2OrderArg}  --formats "${umapClusteringFormats}" \\
  --normalize "${umapClusteringNormalize}" \\
  --transform "${umapClusteringTransform}" \\
  --n-neighbors ${umapClusteringNeighbors} \\
  --min-dist ${umapClusteringMinDist} \\
  --umap-metric "${umapClusteringMetric}" \\
  --min-cluster-size ${umapClusteringMinClusterSize} \\
  --min-samples ${umapClusteringMinSamples} \\
  --hdbscan-metric "${umapClusteringHdbscanMetric}" \\
${umapNoScaleArg}  --random-state ${umapClusteringRandomState}

touch umap_clustering.done
"""
}

process ASV_BATCH_CORRECTION {
    cpus pipelineThreads
    conda "${batchCorrectionCondaEnvPath}"

    input:
    path(metadata_table)
    path(asv_meta)
    path(asv_counts)

    output:
    path("asv_clr_selected.tsv"), emit: asv_clr_selected
    path("asv_clr_after_correction.tsv"), emit: asv_clr_after
    path("asv_corrected_abundance.features_rows.tsv"), emit: asv_corrected_counts
    path("asv_corrected_pseudocount.features_rows.tsv"), emit: asv_corrected_counts_int
    path("asv_selected_abundance.features_rows.tsv"), emit: asv_selected_counts
    path("asv_selected_pseudocount.features_rows.tsv"), emit: asv_selected_counts_int
    path("batch_correction_decision.tsv"), emit: correction_decision
    path("batch_correction_countspace_preservation.png"), emit: countspace_plot
    path("batch_correction_countspace_preservation_metrics.tsv"), emit: countspace_metrics
    path("batch_correction_umap_comparison.png"), emit: umap_plot
    path("batch_correction_statistics.tsv"), emit: correction_stats
    path("umap_hdbscan_results.tsv"), emit: umap_results

    when:
    batchCorrectionEnabled

    script:
    def bioCovArg = batchBiologicalCovariates ? """  --biological-covariates "${batchBiologicalCovariates}" \\\n""" : ''
    def minSamplesArg = batchHdbscanMinSamples != null ? "  --hdbscan-min-samples ${batchHdbscanMinSamples} \\\n" : ''
    def optimizeFlag = batchOptimize ? "  --optimize-clustering \\\n" : ''
    def conqurBatchRefArg = batchConqurBatchRef ? """  --conqur-batch-ref "${batchConqurBatchRef}" \\\n""" : ''
    def conqurLogisticLassoFlag = batchConqurLogisticLasso ? "  --conqur-logistic-lasso \\\n" : ''
    def conqurSimpleMatchFlag = batchConqurSimpleMatch ? "  --conqur-simple-match \\\n" : ''
    def conqurInterpltFlag = batchConqurInterplt ? "  --conqur-interplt \\\n" : ''
    def conqurAutoInstallFlag = batchConqurAutoInstall ? "  --conqur-auto-install \\\n" : ''
    def asvClrAfterFile = "${batchCorrectionOutputDirAbs}/asv_clr_after_correction.tsv"
    def asvCorrectedFeaturesFile = "${batchCorrectionOutputDirAbs}/asv_corrected_abundance.features_rows.tsv"
    def asvCorrectedPseudoFeaturesFile = "${batchCorrectionOutputDirAbs}/asv_corrected_pseudocount.features_rows.tsv"
    def asvSelectedClrFile = "${batchCorrectionOutputDirAbs}/asv_clr_selected.tsv"
    def asvSelectedFeaturesFile = "${batchCorrectionOutputDirAbs}/asv_selected_abundance.features_rows.tsv"
    def asvSelectedPseudoFeaturesFile = "${batchCorrectionOutputDirAbs}/asv_selected_pseudocount.features_rows.tsv"
    def batchCorrectionDecisionFile = "${batchCorrectionOutputDirAbs}/batch_correction_decision.tsv"
    def countspacePlotFile = "${batchCorrectionOutputDirAbs}/batch_correction_countspace_preservation.png"
    def countspaceMetricsFile = "${batchCorrectionOutputDirAbs}/batch_correction_countspace_preservation_metrics.tsv"
    def umapComparisonPngFile = "${batchCorrectionOutputDirAbs}/batch_correction_umap_comparison.png"
    def batchCorrectionStatsFile = "${batchCorrectionOutputDirAbs}/batch_correction_statistics.tsv"
    def umapResultsFile = "${batchCorrectionOutputDirAbs}/umap_hdbscan_results.tsv"
    """
set -euo pipefail

python "${batchCorrectionScriptPath}" \\
  --data-dir "${outputDir}" \\
  --asv "\$PWD/${asv_counts}" \\
  --metadata "\$PWD/${metadata_table}" \\
  --asv-meta "\$PWD/${asv_meta}" \\
  --sample-id-col "${batchCorrectionSampleIdCol}" \\
  --batch-col "${batchCorrectionBatchCol}" \\
  --output-dir "${batchCorrectionOutputDir}" \\
  --asv-orientation "${batchCorrectionOrientation}" \\
  --conqur-mode "${batchConqurMode}" \\
  --correction-policy "${batchCorrectionPolicy}" \\
  --auto-min-sample-rho ${batchAutoMinSampleRho} \\
  --auto-min-bray-rho ${batchAutoMinBrayRho} \\
  --auto-max-batch-eta-ratio ${batchAutoMaxBatchEtaRatio} \\
  --auto-min-batch-eta-drop ${batchAutoMinBatchEtaDrop} \\
  --auto-min-bio-eta-ratio ${batchAutoMinBioEtaRatio} \\
  --conqur-num-core ${batchConqurNumCore} \\
${conqurBatchRefArg}${conqurLogisticLassoFlag}  --conqur-quantile-type "${batchConqurQuantileType}" \\
${conqurSimpleMatchFlag}  --conqur-lambda-quantile "${batchConqurLambdaQuantile}" \\
${conqurInterpltFlag}  --conqur-delta ${batchConqurDelta} \\
${conqurAutoInstallFlag}${bioCovArg}  --umap-neighbors ${batchUmapNeighbors} \\
  --umap-min-dist ${batchUmapMinDist} \\
  --hdbscan-min-cluster-size ${batchHdbscanMinClusterSize} \\
${minSamplesArg}  --hdbscan-selection-method "${batchHdbscanSelectionMethod}" \\
  --target-clusters "${batchTargetClusters}" \\
  --n-features-plot ${batchNFeaturesPlot} \\
  --biological-color-col "${batchBiologicalColorCols}" \\
  --color-palette-col "${batchColorPaletteCols}" \\
  --biological-palettes-json '${batchBiologicalPalettesJson}' \\
  --random-state ${batchRandomState} \\
${optimizeFlag}  --verbose

if [[ ! -f "${asvClrAfterFile}" ]]; then
  echo "Missing batch correction output: ${asvClrAfterFile}" >&2
  exit 1
fi
ln -sf "${asvClrAfterFile}" asv_clr_after_correction.tsv
if [[ ! -f "${asvCorrectedFeaturesFile}" ]]; then
  echo "Missing batch correction output: ${asvCorrectedFeaturesFile}" >&2
  exit 1
fi
ln -sf "${asvCorrectedFeaturesFile}" asv_corrected_abundance.features_rows.tsv
if [[ ! -f "${asvCorrectedPseudoFeaturesFile}" ]]; then
  echo "Missing batch correction output: ${asvCorrectedPseudoFeaturesFile}" >&2
  exit 1
fi
ln -sf "${asvCorrectedPseudoFeaturesFile}" asv_corrected_pseudocount.features_rows.tsv
if [[ ! -f "${asvSelectedClrFile}" ]]; then
  echo "Missing batch correction selected output: ${asvSelectedClrFile}" >&2
  exit 1
fi
ln -sf "${asvSelectedClrFile}" asv_clr_selected.tsv
if [[ ! -f "${asvSelectedFeaturesFile}" ]]; then
  echo "Missing batch correction selected output: ${asvSelectedFeaturesFile}" >&2
  exit 1
fi
ln -sf "${asvSelectedFeaturesFile}" asv_selected_abundance.features_rows.tsv
if [[ ! -f "${asvSelectedPseudoFeaturesFile}" ]]; then
  echo "Missing batch correction selected output: ${asvSelectedPseudoFeaturesFile}" >&2
  exit 1
fi
ln -sf "${asvSelectedPseudoFeaturesFile}" asv_selected_pseudocount.features_rows.tsv
if [[ ! -f "${batchCorrectionDecisionFile}" ]]; then
  echo "Missing batch correction decision output: ${batchCorrectionDecisionFile}" >&2
  exit 1
fi
ln -sf "${batchCorrectionDecisionFile}" batch_correction_decision.tsv
if [[ ! -f "${countspacePlotFile}" ]]; then
  echo "Missing batch correction output: ${countspacePlotFile}" >&2
  exit 1
fi
ln -sf "${countspacePlotFile}" batch_correction_countspace_preservation.png
if [[ ! -f "${countspaceMetricsFile}" ]]; then
  echo "Missing batch correction output: ${countspaceMetricsFile}" >&2
  exit 1
fi
ln -sf "${countspaceMetricsFile}" batch_correction_countspace_preservation_metrics.tsv
if [[ ! -f "${umapComparisonPngFile}" ]]; then
  echo "Missing batch correction output: ${umapComparisonPngFile}" >&2
  exit 1
fi
ln -sf "${umapComparisonPngFile}" batch_correction_umap_comparison.png
if [[ ! -f "${batchCorrectionStatsFile}" ]]; then
  echo "Missing batch correction output: ${batchCorrectionStatsFile}" >&2
  exit 1
fi
ln -sf "${batchCorrectionStatsFile}" batch_correction_statistics.tsv
if [[ ! -f "${umapResultsFile}" ]]; then
  echo "Missing batch correction output: ${umapResultsFile}" >&2
  exit 1
fi
ln -sf "${umapResultsFile}" umap_hdbscan_results.tsv
"""
}

process ASV_META_FROM_CORRECTED {
    cpus 1
    conda "${batchCorrectionCondaEnvPath}"

    input:
    path(asv_meta)
    path(corrected_counts)

    output:
    path("ASV_meta_micro.corrected.tsv"), emit: asv_meta_corrected

    when:
    batchCorrectionEnabled && (bubbleplotterEnabled || umapClusteringEnabled || clustermapsEnabled)

    script:
    """
set -euo pipefail

python - "${asv_meta}" "${corrected_counts}" "ASV_meta_micro.corrected.tsv" <<'PY'
import sys
import pandas as pd

asv_meta_path, corrected_counts_path, out_path = sys.argv[1:4]
sample_col = "${batchCorrectionSampleIdCol}"
asv_col = "ASV_ID"
count_col = "count"
count_alias_col = "corr_count"

meta = pd.read_csv(asv_meta_path, sep='\\t')
if sample_col not in meta.columns or asv_col not in meta.columns:
    raise ValueError(f"Expected columns '{sample_col}' and '{asv_col}' in {asv_meta_path}")

corr = pd.read_csv(corrected_counts_path, sep='\\t', index_col=0)
corr.index = corr.index.astype(str)
corr.columns = corr.columns.astype(str)

corr_long = corr.stack().rename(count_col).reset_index()
corr_long.columns = [asv_col, sample_col, count_col]
corr_long[count_col] = pd.to_numeric(corr_long[count_col], errors='coerce').fillna(0.0).clip(lower=0.0)

candidate_cols = [c for c in meta.columns if c not in {sample_col, asv_col, count_col, count_alias_col}]
asv_only_cols = []
sample_only_cols = []
for col in candidate_cols:
    asv_n = meta.groupby(asv_col, dropna=False)[col].nunique(dropna=False).max()
    sample_n = meta.groupby(sample_col, dropna=False)[col].nunique(dropna=False).max()
    if asv_n <= 1 and sample_n > 1:
        asv_only_cols.append(col)
    else:
        sample_only_cols.append(col)

sample_meta = meta[[sample_col] + sample_only_cols].drop_duplicates(subset=[sample_col])
asv_meta_df = meta[[asv_col] + asv_only_cols].drop_duplicates(subset=[asv_col])

out = corr_long.merge(sample_meta, on=sample_col, how='left')
out = out.merge(asv_meta_df, on=asv_col, how='left')
out = out[out[count_col] > 0]
out[count_alias_col] = out[count_col]

front = [sample_col, asv_col, count_col, count_alias_col]
remaining = [c for c in out.columns if c not in front]
out = out[front + remaining]
out.to_csv(out_path, sep='\\t', index=False)

print(f"[i] Wrote corrected ASV meta table: {out_path}")
print(f"[i] Rows: {len(out)}")
PY
"""
}

process OUTLIER_CHECKER {
    cpus pipelineThreads
    conda "${outlierCondaEnvPath}"

    input:
    path(asv_clr)
    path(metadata_table)

    when:
    outlierEnabled

    script:
    def groupColsArg = outlierGroupCols.join(',')
    def isoFlag = outlierUseIso ? "  --use-iso \\\n" : ''
    def svmFlag = outlierUseSvm ? "  --use-svm \\\n" : ''
    def hdbFlag = outlierUseHdb ? "  --use-hdb \\\n" : ''
    def preTransFlag = outlierPreTransformed ? "  --pre-transformed \\\n" : ''
    def scaleFlag = outlierScale ? "  --scale \\\n" : ''
    def hdbMinSamplesArg = outlierHdbMinSamples != null ? "  --hdbscan-min-samples ${outlierHdbMinSamples} \\\n" : ''
    def updated_metadata = "metadata/${metadata_table}"
    def asvClrAfterFile = "${batchCorrectionOutputDirAbs}/asv_clr_after_correction.tsv"


    """
set -euo pipefail

python "${outlierCheckerScriptPath}" \\
  --data-dir "${outputDir}" \\
  --asv "${asvClrAfterFile}" \\
  --metadata "${updated_metadata}" \\
  --sample-id-col "${outlierSampleIdCol}" \\
  --output-dir "${outlierOutputDirAbs}" \\
  --group-cols "${groupColsArg}" \\
  --asv-orientation "${outlierOrientation}" \\
  --transform "${outlierTransform}" \\
${preTransFlag}${scaleFlag}${isoFlag}${svmFlag}${hdbFlag}  --vote-threshold ${outlierVoteThreshold} \\
  --iso-contamination "${outlierIsoContamination}" \\
  --iso-estimators ${outlierIsoEstimators} \\
  --iso-random-state ${outlierIsoRandomState} \\
  --svm-kernel "${outlierSvmKernel}" \\
  --svm-gamma "${outlierSvmGamma}" \\
  --svm-nu ${outlierSvmNu} \\
  --hdbscan-min-cluster-size ${outlierHdbMinClusterSize} \\
${hdbMinSamplesArg}  --hdbscan-metric "${outlierHdbMetric}" \\
  --verbose
"""
}

process COLLECTORS_CURVE {
    cpus pipelineThreads
    conda "${collectorsCondaEnvPath}"

    input:
    path(asv_counts)
    path(metadata_table)

    when:
    collectorsEnabled

    script:
    def collectorsGroupOrderArg = collectorsGroupOrder && !collectorsGroupOrder.isEmpty() ? """  --group-order "${collectorsGroupOrder.join(',')}" \\\n""" : ''
    def collectorsGroupColorsArg = collectorsGroupColors ? """  --group-colors "${collectorsGroupColors}" \\\n""" : ''
    """
set -euo pipefail

python "${collectorsCurveScriptPath}" \\
  --counts "${asv_counts}" \\
  --meta "${metadata_table}" \\
  --sample-col "${collectorsSampleCol}" \\
  --group-col "${collectorsGroupCol}" \\
  --color-col "${collectorsColorCol}" \\
${collectorsGroupColorsArg}${collectorsGroupOrderArg}  --permutations ${collectorsPermutations} \\
  --seed ${collectorsSeed} \\
  --out_prefix "${collectorsOutPrefixAbs}" \\
  --title "${collectorsTitle}" \\
  --formats "${collectorsFormats}" \\
  --xpad ${collectorsXpad} \\
  --max-cols ${collectorsMaxCols} \\
  --show-perms ${collectorsShowPerms} \\
  --presence-threshold ${collectorsPresenceThreshold}
"""
}

process DIVERSITY_ANALYSIS {
    cpus pipelineThreads
    conda "${diversityCondaEnvPath}"

    input:
    path(metadata_table)
    path(asv_counts)

    output:
    path("diversity.done"), emit: done

    when:
    diversityEnabled

    script:
    def secondaryColArg = diversitySecondaryCol ? """  --secondary-col "${diversitySecondaryCol}" \\\n""" : ''
    def excludeGroupsArg = diversityExcludeGroups && !diversityExcludeGroups.isEmpty() ? """  --exclude-groups "${diversityExcludeGroups.join(',')}" \\\n""" : ''
    def groupOrderArg = diversityGroupOrder && !diversityGroupOrder.isEmpty() ? """  --group-order "${diversityGroupOrder.join(',')}" \\\n""" : ''
    def diversityBlockArg = diversityBlockCol ? """  --block-col "${diversityBlockCol}" \\\n""" : ''
    def verboseFlag = diversityVerbose ? "  --verbose\n" : ''
    def diversityRunMitoFlag = diversityRunMito ? '1' : '0'
    def diversityPatientAwareEnabledFlag = diversityPatientAwareEnabled ? '1' : '0'
    def diversityPatientAwareExcludeContralateralFlag = diversityPatientAwareExcludeContralateral ? 'TRUE' : 'FALSE'
    def diversityPatientAwareRequireCompleteTypesFlag = diversityPatientAwareRequireCompleteTypes ? '1' : '0'
    """
set -euo pipefail
mkdir -p "${diversityOutputDirAbs}"
mkdir -p "${diversityMitoOutputDirAbs}"

if [[ "${diversityRunMitoFlag}" == "1" && -f "${diversityMitoInputPath}" ]]; then
  python "${calcDivScriptPath}" \\
    --micro-table "${asv_counts}" \\
    --mito-table "${diversityMitoInputPath}" \\
    --outdir "${diversityOutputDirAbs}" \\
    --mito-outdir "${diversityMitoOutputDirAbs}"
else
  python "${calcDivScriptPath}" \\
    --micro-table "${asv_counts}" \\
    --outdir "${diversityOutputDirAbs}"
fi

python "${plotDiversityScriptPath}" \\
  --metadata "${metadata_table}" \\
  --sample-col "${diversitySampleCol}" \\
  --group-col "${diversityGroupCol}" \\
  --color-col "${diversityColorCol}" \\
  --group-palette "${diversityGroupPalette}" \\
  --secondary-palette "${diversitySecondaryPalette}" \\
${secondaryColArg}${excludeGroupsArg}${groupOrderArg}  --alpha-table "${diversityOutputDirAbs}/shannon.tsv" \\
  --distance-bray "${diversityOutputDirAbs}/bray.tsv" \\
  --distance-jaccard "${diversityOutputDirAbs}/jaccard.tsv" \\
  --output-dir "${diversityOutputDirAbs}" \\
  --umap-neighbors ${diversityUmapNeighbors} \\
  --umap-min-dist ${diversityUmapMinDist} \\
${diversityBlockArg}  --permanova-perms ${diversityPermutations} \\
  --random-state ${diversityRandomState} \\
${verboseFlag}

if [[ "${diversityRunMitoFlag}" == "1" && -f "${diversityMitoOutputDirAbs}/shannon.mito.tsv" && -f "${diversityMitoOutputDirAbs}/bray.mito.tsv" && -f "${diversityMitoOutputDirAbs}/jaccard.mito.tsv" ]]; then
  python "${plotDiversityScriptPath}" \\
    --metadata "${metadata_table}" \\
    --sample-col "${diversitySampleCol}" \\
    --group-col "${diversityGroupCol}" \\
    --color-col "${diversityColorCol}" \\
    --group-palette "${diversityGroupPalette}" \\
    --secondary-palette "${diversitySecondaryPalette}" \\
${secondaryColArg}${excludeGroupsArg}${groupOrderArg}    --alpha-table "${diversityOutputDirAbs}/shannon.tsv" \\
    --distance-bray "${diversityOutputDirAbs}/bray.tsv" \\
    --distance-jaccard "${diversityOutputDirAbs}/jaccard.tsv" \\
    --output-dir "${diversityOutputDirAbs}" \\
    --mito-mode \\
    --mito-alpha "${diversityMitoOutputDirAbs}/shannon.mito.tsv" \\
    --mito-bray "${diversityMitoOutputDirAbs}/bray.mito.tsv" \\
    --mito-jaccard "${diversityMitoOutputDirAbs}/jaccard.mito.tsv" \\
    --mito-output-dir "${diversityMitoOutputDirAbs}" \\
    --umap-neighbors ${diversityUmapNeighbors} \\
    --umap-min-dist ${diversityUmapMinDist} \\
${diversityBlockArg}    --permanova-perms ${diversityPermutations} \\
    --random-state ${diversityRandomState} \\
${verboseFlag}
fi

if [[ "${diversityPatientAwareEnabledFlag}" == "1" ]]; then
  mkdir -p "${diversityPatientAwareOutputDirAbs}"

  DIVERSITY_PATIENT_AWARE_ARGS=()
  if [[ "${diversityPatientAwareRequireCompleteTypesFlag}" == "1" ]]; then
    DIVERSITY_PATIENT_AWARE_ARGS+=(--require-complete-types)
  fi

  Rscript "${brayPatientAwareScriptPath}" \\
    --data-wide "${asv_counts}" \\
    --data-long "${metadata_table}" \\
    --sample-col "${diversityPatientAwareSampleCol}" \\
    --patient-col "${diversityPatientAwarePatientCol}" \\
    --case-col "${diversityPatientAwareCaseCol}" \\
    --type-col "${diversityPatientAwareTypeCol}" \\
    --sample-types "${diversityPatientAwareSampleTypes}" \\
    --contralateral-col "${diversityPatientAwareContralateralCol}" \\
    --cancer-site-col "${diversityPatientAwareCancerSiteCol}" \\
    --lung-side-col "${diversityPatientAwareLungSideCol}" \\
    --contralateral-value "${diversityPatientAwareContralateralValue}" \\
    --contralateral-sample-types "${diversityPatientAwareContralateralTypes}" \\
    --exclude-contralateral-in-cancer ${diversityPatientAwareExcludeContralateralFlag} \\
    --transform "${diversityPatientAwareTransform}" \\
    --permutations ${diversityPatientAwarePermutations} \\
    --seed ${diversityPatientAwareSeed} \\
    --outdir "${diversityPatientAwareOutputDirAbs}" \\
    "\${DIVERSITY_PATIENT_AWARE_ARGS[@]}"

  python "${plotBrayPatientAwareScriptPath}" \\
    --indir "${diversityPatientAwareOutputDirAbs}" \\
    --outdir "${diversityPatientAwareOutputDirAbs}/figures"
fi

touch diversity.done
"""
}

process INDICSPECIES {
    cpus pipelineThreads
    conda "${indicspeciesCondaEnvPath}"

    input:
    path(metadata_table)
    path(asv_counts)

    output:
    path("indicspecies_group1_summary.tsv"), emit: group1_summary
    path("indicspecies_group2_summary.tsv"), emit: group2_summary
    path("indicspecies_group1_results.tsv"), emit: group1_results
    path("indicspecies_group2_results.tsv"), emit: group2_results
    path("indicspecies_tables/*.tsv"), emit: all_tables
    path("indicspecies.done"), emit: done

    when:
    indicspeciesEnabled

    script:
    def indicspeciesGroupColsArg = indicspeciesGroupCols.join(',')
    def indicspeciesBlockArg = indicspeciesBlockCol ? """  --block-col "${indicspeciesBlockCol}" \\\n""" : ''
    def indicspeciesStratifiedArg = indicspeciesStratifiedSpecsArg ? """  --stratified-isa "${indicspeciesStratifiedSpecsArg}" \\\n""" : ''
    def isaSummarySuffix = indicspeciesUseDuleg ? '_indicator_species_DULEG_summary.tsv' : '_indicator_species_summary.tsv'
    def isaResultsSuffix = indicspeciesUseDuleg ? '_indicator_species_DULEG_results.tsv' : '_indicator_species_results.tsv'
    def group1SummaryPath = "${indicspeciesOutputDirAbs}/${indicspeciesGroup1}${isaSummarySuffix}"
    def group2SummaryPath = "${indicspeciesOutputDirAbs}/${indicspeciesGroup2}${isaSummarySuffix}"
    def group1ResultsPath = "${indicspeciesOutputDirAbs}/${indicspeciesGroup1}${isaResultsSuffix}"
    def group2ResultsPath = "${indicspeciesOutputDirAbs}/${indicspeciesGroup2}${isaResultsSuffix}"
    """
set -euo pipefail
mkdir -p "${indicspeciesOutputDirAbs}"

Rscript "${indicspeciesScriptPath}" \\
  --asv "${asv_counts}" \\
  --meta "${metadata_table}" \\
  --sample-col "${indicspeciesSampleCol}" \\
  --group-cols "${indicspeciesGroupColsArg}" \\
${indicspeciesBlockArg}${indicspeciesStratifiedArg}  --perms ${indicspeciesPerms} \\
  --seed ${indicspeciesSeed} \\
  --q-threshold ${indicspeciesQThreshold} \\
  --min-n ${indicspeciesMinN} \\
  --outdir "${outputDir}"

python - <<'PY'
from pathlib import Path
import sys

group1_summary = Path("${group1SummaryPath}")
group2_summary = Path("${group2SummaryPath}")
group1_results = Path("${group1ResultsPath}")
group2_results = Path("${group2ResultsPath}")
required = [group1_summary, group2_summary, group1_results, group2_results]

missing = [str(p) for p in required if not p.is_file()]
if missing:
    for p in missing:
        print(f"Missing expected indicspecies output: {p}", file=sys.stderr)
    raise SystemExit(1)

tables_dir = Path("indicspecies_tables")
tables_dir.mkdir(parents=True, exist_ok=True)
all_tables = sorted(Path("${indicspeciesOutputDirAbs}").glob("*_indicator_species*.tsv"))
if not all_tables:
    print("No indicspecies tables were generated in ${indicspeciesOutputDirAbs}", file=sys.stderr)
    raise SystemExit(1)

for src in all_tables:
    dst = tables_dir / src.name
    if dst.exists() or dst.is_symlink():
        dst.unlink()
    dst.symlink_to(src.resolve())

link_map = {
    "indicspecies_group1_summary.tsv": group1_summary,
    "indicspecies_group2_summary.tsv": group2_summary,
    "indicspecies_group1_results.tsv": group1_results,
    "indicspecies_group2_results.tsv": group2_results,
}
for dst_name, src in link_map.items():
    dst = Path(dst_name)
    if dst.exists() or dst.is_symlink():
        dst.unlink()
    dst.symlink_to(src.resolve())
PY

touch indicspecies.done
"""
}

process INDICSPECIES_PLOTS {
    cpus pipelineThreads
    conda "${indicspeciesCondaEnvPath}"

    input:
    path(metadata_table)
    path(indicspecies_tables)

    output:
    path("indicspecies_plots.done"), emit: done

    when:
    indicspeciesEnabled && indicspeciesPlotEnabled

    script:
    def plotVennPath = indicspeciesPlotVennPath ?: ''
    def plotTaxPath = indicspeciesPlotTaxonomyPath ?: ''
    def plotPairsMode = indicspeciesPlotPairsMode?.toString()?.trim()?.toLowerCase() ?: 'all'
    """
set -euo pipefail
mkdir -p "${indicspeciesPlotOutputDirAbs}"

python - <<'PY'
from pathlib import Path
import itertools
import json
import re
import subprocess
import sys

plot_script = Path("${plotIndicspeciesScriptPath}")
out_root = Path("${indicspeciesPlotOutputDirAbs}")
pairs_mode = "${plotPairsMode}"
plot_tax = Path("${plotTaxPath}") if "${plotTaxPath}" else None
plot_venn = Path("${plotVennPath}") if "${plotVennPath}" else None
metadata_path = Path("${metadata_table}") if "${metadata_table}" else None
preferred_group1 = "${indicspeciesGroup1}".strip()
preferred_group2 = "${indicspeciesGroup2}".strip()
metadata_color_col = "${indicspeciesColorCol}".strip()
palette_cfg = json.loads(r'''${indicspeciesGroupPaletteJson}''')
order_cfg = json.loads(r'''${indicspeciesGroupOrderJson}''')
focus_cfg = json.loads(r'''${indicspeciesFocusLabelJson}''')
label_focused_asvs = ${indicspeciesLabelFocusedAsvs ? 'True' : 'False'}

summary_files = sorted(Path(".").glob("*_indicator_species*_summary.tsv"))
if len(summary_files) < 2:
    print(
        f"[w] Need at least 2 indicspecies summary tables to build ISA plots; found {len(summary_files)}. Skipping.",
        file=sys.stderr,
    )
    raise SystemExit(0)

def clean_group_name(raw_name: str) -> str:
    name = raw_name
    for ending in ("_summary.tsv", "_results.tsv"):
        if name.endswith(ending):
            name = name[: -len(ending)]
            break
    for suffix in ("_indicator_species_DULEG", "_indicator_species"):
        if name.endswith(suffix):
            name = name[: -len(suffix)]
    return name

def orient_pair(a: Path, b: Path) -> tuple[Path, Path]:
    an = clean_group_name(a.name)
    bn = clean_group_name(b.name)
    if an == preferred_group1 and bn == preferred_group2:
        return a, b
    if an == preferred_group2 and bn == preferred_group1:
        return b, a
    if an == preferred_group1:
        return a, b
    if bn == preferred_group1:
        return b, a
    return a, b

if pairs_mode == "first_vs_rest":
    raw_pairs = [(summary_files[0], f) for f in summary_files[1:]]
else:
    raw_pairs = list(itertools.combinations(summary_files, 2))
pair_iter = [orient_pair(a, b) for a, b in raw_pairs]

def has_col(path: Path, col: str) -> bool:
    if not col:
        return False
    try:
        header = path.read_text(encoding="utf-8").splitlines()[0].split("\t")
    except Exception:
        return False
    return col in header

def has_meta_col(path: Path | None, col: str) -> bool:
    if path is None or not path.is_file() or not col:
        return False
    return has_col(path, col)

def order_string(group_name: str) -> str:
    raw = order_cfg.get(group_name, [])
    if isinstance(raw, list):
        return ",".join(str(x).strip() for x in raw if str(x).strip())
    if raw:
        return str(raw).strip()
    return ""

for g1_file, g2_file in pair_iter:
    g1_name = clean_group_name(g1_file.name)
    g2_name = clean_group_name(g2_file.name)
    if g1_name == g2_name:
        print(
            f"[w] Skipping same-group ISA pair: {g1_file.name} vs {g2_file.name}",
            file=sys.stderr,
        )
        continue
    pair_slug = re.sub(r"[^0-9A-Za-z._-]", "_", f"{g1_name}__{g2_name}")
    pair_out = out_root / pair_slug
    pair_out.mkdir(parents=True, exist_ok=True)

    cmd = [
        "python",
        str(plot_script),
        "--group1-results",
        str(g1_file),
        "--group2-results",
        str(g2_file),
        "--group1-name",
        g1_name,
        "--group2-name",
        g2_name,
        "--outdir",
        str(pair_out),
    ]
    if metadata_path and metadata_path.is_file():
        cmd.extend(["--metadata", str(metadata_path)])
        if has_meta_col(metadata_path, g1_name):
            cmd.extend(["--group1-meta-label-col", g1_name])
        if has_meta_col(metadata_path, g2_name):
            cmd.extend(["--group2-meta-label-col", g2_name])
        if g1_name == preferred_group1 and metadata_color_col and has_meta_col(metadata_path, metadata_color_col):
            cmd.extend(["--group1-meta-color-col", metadata_color_col])

    if has_col(g1_file, g1_name):
        cmd.extend(["--group1-label-col", g1_name])
    if has_col(g1_file, f"{g1_name}_Color"):
        cmd.extend(["--group1-color-col", f"{g1_name}_Color"])
    elif has_col(g1_file, "Color"):
        cmd.extend(["--group1-color-col", "Color"])

    if has_col(g2_file, g2_name):
        cmd.extend(["--group2-label-col", g2_name])
    if has_col(g2_file, f"{g2_name}_Color"):
        cmd.extend(["--group2-color-col", f"{g2_name}_Color"])
    elif has_col(g2_file, "Color"):
        cmd.extend(["--group2-color-col", "Color"])

    if has_col(g2_file, f"{g2_name}_Marker"):
        cmd.extend(["--group2-marker-col", f"{g2_name}_Marker"])
    if palette_cfg.get(g1_name):
        cmd.extend(["--group1-palette", str(palette_cfg[g1_name])])
    if palette_cfg.get(g2_name):
        cmd.extend(["--group2-palette", str(palette_cfg[g2_name])])
    group1_order_cfg = order_string(g1_name)
    group2_order_cfg = order_string(g2_name)
    if group1_order_cfg:
        cmd.extend(["--group1-order", group1_order_cfg])
    if group2_order_cfg:
        cmd.extend(["--group2-order", group2_order_cfg])
    if focus_cfg.get(g1_name):
        cmd.extend(["--focus-group1-label", str(focus_cfg[g1_name])])
    if focus_cfg.get(g2_name):
        cmd.extend(["--focus-group2-label", str(focus_cfg[g2_name])])
    if label_focused_asvs:
        cmd.append("--label-focused-asvs")
    if plot_tax and plot_tax.is_file():
        cmd.extend(["--taxonomy", str(plot_tax)])
    if plot_venn and plot_venn.is_file():
        cmd.extend(["--venn", str(plot_venn)])

    subprocess.run(cmd, check=True)
PY

touch indicspecies_plots.done
"""
}

process INDICSPECIES_ALIGNED_PLOTS {
    cpus pipelineThreads
    conda "${indicspeciesCondaEnvPath}"

    input:
    path(indicspecies_tables)

    output:
    path("indicspecies_aligned.done"), emit: done

    when:
    indicspeciesEnabled && indicspeciesAlignedEnabled

    script:
    """
set -euo pipefail
mkdir -p "${indicspeciesAlignedOutputDirAbs}"
mkdir -p aligned_indicspecies_input

for src in *.tsv; do
  [[ -f "\${src}" ]] || continue
  ln -sf "\$(realpath "\${src}")" "aligned_indicspecies_input/\$(basename "\${src}")"
done

python "${plotIndicspeciesAlignedScriptPath}" \\
  --indicspecies-dir aligned_indicspecies_input \\
  --outdir "${indicspeciesAlignedOutputDirAbs}" \\
  --alpha ${indicspeciesAlignedAlpha} \\
  --min-stat ${indicspeciesAlignedMinStat} \\
  --top-n ${indicspeciesAlignedTopN}

touch indicspecies_aligned.done
"""
}

process VOC_CORRELATION {
    cpus pipelineThreads
    conda "${vocCorrelationCondaEnvPath}"

    input:
    path(asv_meta_table)
    path(asv_counts)
    path(indicspecies_tables)

    output:
    path("voc_correlation.done"), emit: done

    when:
    vocCorrelationEnabled

    script:
    def vocColsArgs = vocCorrelationVocCols.collect { col -> """  --voc-col "${col}" \\\n""" }.join('')
    def legacySubsetArg = vocCorrelationUseLegacySubset ? "  --use-legacy-voc-subset \\\n" : ''
"""
set -euo pipefail
mkdir -p "${vocCorrelationOutputDirAbs}"
echo "plot_voc_corr.py md5: ${plotVocCorrScriptHash}"

python3 "${plotVocCorrScriptPath}" \\
  --asv-meta "${asv_meta_table}" \\
  --asv-counts "${asv_counts}" \\
  --voc "${vocCorrelationVocTablePath}" \\
  --outdir "${vocCorrelationOutputDirAbs}" \\
  --metadata-sample-col "${vocCorrelationMetadataSampleCol}" \\
  --type-col "${vocCorrelationTypeCol}" \\
  --patient-col "${vocCorrelationPatientCol}" \\
  --case-col "${vocCorrelationCaseCol}" \\
  --sample-types "${vocCorrelationSampleTypes}" \\
  --voc-sample-col "${vocCorrelationVocSampleCol}" \\
  --sample-id-mode "${vocCorrelationSampleIdMode}" \\
${legacySubsetArg}${vocColsArgs}  --spieceasi-min-rel-abund ${spieceasiMinRelAbund} \\
  --spieceasi-min-prevalence ${spieceasiMinPrevalence} \\
  --spieceasi-remove-zero-var ${spieceasiRemoveZeroVar} \\
  --correlation-direction "${vocCorrelationDirection}" \\
  --case-palette "${vocCorrelationCasePalette}" \\
  --isa-palette "${vocCorrelationIsaPalette}" \\
  --isa-q-threshold ${indicspeciesQThreshold} \\
  --indicspecies-glob "*_indicator_species*.tsv"

touch voc_correlation.done
"""
}

process MEASUREMENT_ASSOCIATION {
    cpus pipelineThreads
    conda "${measurementAssociationCondaEnvPath}"

    input:
    path(asv_meta_table)
    path(metadata_table)
    path(asv_counts)

    output:
    path("measurement_association.done"), emit: done

    when:
    measurementAssociationEnabled

    script:
    def measurementTableArg = measurementAssociationTablePath ? """  --measurement-table "${measurementAssociationTablePath}" \\\n""" : ''
    def measurementColsArg = measurementAssociationCols ? """  --measurement-cols "${measurementAssociationCols.join('|')}" \\\n""" : ''
    def excludeColsArg = measurementAssociationExcludeCols ? """  --exclude-cols "${measurementAssociationExcludeCols.join('|')}" \\\n""" : ''
    def metadataJoinArg = measurementAssociationMetadataJoinCols ? measurementAssociationMetadataJoinCols.join(',') : ''
    def measurementJoinArg = measurementAssociationMeasurementJoinCols ? measurementAssociationMeasurementJoinCols.join(',') : ''
"""
set -euo pipefail
mkdir -p "${measurementAssociationOutputDirAbs}"
echo "measurement_association.py md5: ${measurementAssociationScriptHash}"
echo "run_measurement_association.R md5: ${measurementAssociationRScriptHash}"

python "${measurementAssociationScriptPath}" \\
  --asv-meta "${asv_meta_table}" \\
  --metadata "${metadata_table}" \\
  --asv-counts "${asv_counts}" \\
${measurementTableArg}  --outdir "${measurementAssociationOutputDirAbs}" \\
  --r-script "${measurementAssociationRScriptPath}" \\
  --sample-col "${measurementAssociationSampleCol}" \\
  --asv-id-col "${measurementAssociationAsvIdCol}" \\
  --measurement-sample-col "${measurementAssociationMeasurementSampleCol}" \\
  --metadata-join-cols "${metadataJoinArg}" \\
  --measurement-join-cols "${measurementJoinArg}" \\
${measurementColsArg}${excludeColsArg}  --group-col "${measurementAssociationGroupCol}" \\
  --group-palette "${measurementAssociationGroupPalette}" \\
  --max-asvs ${measurementAssociationMaxAsvs} \\
  --min-total ${measurementAssociationMinTotal} \\
  --min-prevalence ${measurementAssociationMinPrevalence} \\
  --top-correlations ${measurementAssociationTopCorrelations} \\
  --correlation-direction "${measurementAssociationDirection}" \\
  --ordination-methods "${measurementAssociationMethods}" \\
  --permutations ${measurementAssociationPermutations} \\
  --top-vectors ${measurementAssociationTopVectors} \\
  --formats "${measurementAssociationFormats}"

touch measurement_association.done
"""
}

process GROUPING_DIAGNOSTICS {
    cpus pipelineThreads
    conda "${groupingDiagnosticsCondaEnvPath}"

    input:
    path(metadata_table)
    path(asv_counts)

    output:
    path("grouping_diagnostics.done"), emit: done
    path("grouping_soft_label_assignments.tsv"), optional: true, emit: soft_assignments
    path("grouping_soft_label_validation.tsv"), optional: true, emit: soft_validation
    path("grouping_soft_label_validation_summary.tsv"), optional: true, emit: soft_validation_summary

    when:
    groupingDiagnosticsEnabled

    script:
    def groupColsArg = groupingDiagnosticsGroupCols.join(',')
    def softLabelTargetColsArg = groupingDiagnosticsSoftLabelTargetCols.join(',')
    def softLabelExcludeArg = groupingDiagnosticsSoftLabelExcludeLabels.join(',')
    def softLabelArg = groupingDiagnosticsSoftLabelEnabled ? """  --soft-label-missing \\\n  --soft-label-k ${groupingDiagnosticsSoftLabelK} \\\n  --soft-label-group-cols "${softLabelTargetColsArg}" \\\n  --soft-label-exclude-labels "${softLabelExcludeArg}" \\\n  --soft-label-min-class-samples ${groupingDiagnosticsSoftLabelMinClassSamples} \\\n  --soft-label-distance-quantile ${groupingDiagnosticsSoftLabelDistanceQuantile} \\\n""" : ''
    def powerArg = groupingDiagnosticsPowerEnabled ? """  --power-enabled \\\n  --power-sample-sizes "${groupingDiagnosticsPowerSizes}" \\\n  --power-simulations ${groupingDiagnosticsPowerSimulations} \\\n  --power-permutations ${groupingDiagnosticsPowerPermutations} \\\n  --power-alpha ${groupingDiagnosticsPowerAlpha} \\\n""" : ''
"""
set -euo pipefail
mkdir -p "${groupingDiagnosticsOutputDirAbs}"
export MPLCONFIGDIR="\$PWD/.mplconfig"
mkdir -p "\${MPLCONFIGDIR}"
echo "grouping_diagnostics.py md5: ${groupingDiagnosticsScriptHash}"

python "${groupingDiagnosticsScriptPath}" \\
  --metadata "${metadata_table}" \\
  --asv-counts "${asv_counts}" \\
  --outdir "${groupingDiagnosticsOutputDirAbs}" \\
  --sample-col "${groupingDiagnosticsSampleCol}" \\
  --group-cols "${groupColsArg}" \\
  --baseline-group "${groupingDiagnosticsBaselineGroup}" \\
  --primary-group "${groupingDiagnosticsPrimaryGroup}" \\
  --group-palettes-json '${groupingDiagnosticsPaletteJson}' \\
  --group-orders-json '${groupingDiagnosticsOrderJson}' \\
  --metrics "${groupingDiagnosticsMetrics}" \\
  --transform "${groupingDiagnosticsTransform}" \\
  --permutations ${groupingDiagnosticsPermutations} \\
  --random-state ${groupingDiagnosticsRandomState} \\
  --formats "${groupingDiagnosticsFormats}" \\
${softLabelArg}${powerArg}  --power-min-groups ${groupingDiagnosticsPowerMinGroups}

if [[ -f "${groupingDiagnosticsOutputDirAbs}/tables/grouping_soft_label_assignments.tsv" ]]; then
  cp "${groupingDiagnosticsOutputDirAbs}/tables/grouping_soft_label_assignments.tsv" grouping_soft_label_assignments.tsv
  cp "${groupingDiagnosticsOutputDirAbs}/tables/grouping_soft_label_validation.tsv" grouping_soft_label_validation.tsv
  cp "${groupingDiagnosticsOutputDirAbs}/tables/grouping_soft_label_validation_summary.tsv" grouping_soft_label_validation_summary.tsv
fi

touch grouping_diagnostics.done
"""
}

process GROUP_LABEL_AUGMENTATION {
    cpus 1
    conda "${groupLabelAugmentationCondaEnvPath}"
    publishDir "${outputDir}/metadata", mode: 'copy', pattern: '*.augmented.tsv'

    input:
    path(metadata_table)
    path(asv_meta_table)
    path(soft_assignments)
    path(soft_validation_summary)

    output:
    path("metadata_updated_micro.augmented.tsv"), emit: metadata_augmented
    path("ASV_meta_micro.augmented.tsv"), emit: asv_meta_augmented
    path("group_label_augmentation_audit.tsv"), emit: audit
    path("group_label_augmentation.done"), emit: done

    when:
    groupingDiagnosticsApplySoftLabels

    script:
    def excludedLabelsArg = groupingDiagnosticsSoftLabelExcludeLabels.join(',')
    """
set -euo pipefail
echo "group_label_augmentation.py md5: ${groupLabelAugmentationScriptHash}"

python "${groupLabelAugmentationScriptPath}" \\
  --metadata "${metadata_table}" \\
  --asv-meta "${asv_meta_table}" \\
  --assignments "${soft_assignments}" \\
  --validation-summary "${soft_validation_summary}" \\
  --sample-col "${groupingDiagnosticsSampleCol}" \\
  --target-col "${groupingDiagnosticsSoftLabelTargetCol}" \\
  --exclude-labels "${excludedLabelsArg}" \\
  --min-confidence ${groupingDiagnosticsSoftLabelMinConfidence} \\
  --min-neighbor-agreement ${groupingDiagnosticsSoftLabelMinNeighborAgreement} \\
  --min-cv-balanced-accuracy ${groupingDiagnosticsSoftLabelMinCvBalancedAccuracy}

mkdir -p "${groupingDiagnosticsOutputDirAbs}/tables"
cp group_label_augmentation_audit.tsv "${groupingDiagnosticsOutputDirAbs}/tables/group_label_augmentation_audit.tsv"
touch group_label_augmentation.done
"""
}

process GROUP_POWER_ANALYSIS {
    cpus pipelineThreads
    conda "${powerAnalysisCondaEnvPath}"

    input:
    path(asv_meta)
    path(asv_counts)
    val(indicspecies_ready)

    output:
    path("power_analysis.done"), emit: done

    when:
    powerAnalysisEnabled

    script:
    def skipEstimateFlag = powerAnalysisSkipEstimate ? '1' : '0'
    def skipPlotFlag = powerAnalysisSkipPlot ? '1' : '0'
    def keepContralateralFlag = powerAnalysisKeepContralateralInCancer ? '1' : '0'
    """
set -euo pipefail
mkdir -p "${powerAnalysisOutputDirAbs}"

SUMMARY_INPUT_DIR="power_analysis_inputs"
mkdir -p "\${SUMMARY_INPUT_DIR}"

python "${masterSummaryScriptPath}" \\
  --data-dir "${outputDir}" \\
  --asv-meta "${asv_meta}" \\
  --asv-counts "${asv_counts}" \\
  --clustermaps-dir "${clustermapsOutputDirAbs}" \\
  --indicspecies-dir "${powerAnalysisIndicspeciesDir}" \\
  --spieceasi-dir "${spieceasiOutputDirAbs}" \\
  --outdir "\${SUMMARY_INPUT_DIR}" \\
  --max-direct-cols ${masterSummaryMaxDirectCols}

if [[ ! -f "\${SUMMARY_INPUT_DIR}/ASV_master_long.tsv" ]]; then
  echo "Missing expected power-analysis input: \${SUMMARY_INPUT_DIR}/ASV_master_long.tsv" >&2
  exit 1
fi

POWER_INDICSPECIES_ARGS=()
if [[ -d "${powerAnalysisIndicspeciesDir}" ]]; then
  POWER_INDICSPECIES_ARGS=(--indicspecies-dir "${powerAnalysisIndicspeciesDir}")
fi

POWER_SKIP_ARGS=()
if [[ "${skipEstimateFlag}" == "1" ]]; then
  POWER_SKIP_ARGS+=(--skip-estimate)
fi
if [[ "${skipPlotFlag}" == "1" ]]; then
  POWER_SKIP_ARGS+=(--skip-plot)
fi

POWER_CONTRALATERAL_ARGS=()
if [[ "${keepContralateralFlag}" == "1" ]]; then
  POWER_CONTRALATERAL_ARGS+=(--keep-contralateral-in-cancer)
fi

bash "${powerAnalysisScriptPath}" \\
  --data-long "\${SUMMARY_INPUT_DIR}/ASV_master_long.tsv" \\
  --data-wide "${asv_counts}" \\
  --outdir "${powerAnalysisOutputDirAbs}" \\
  --sample-col "${powerAnalysisSampleCol}" \\
  --patient-col "${powerAnalysisPatientCol}" \\
  --case-col "${powerAnalysisCaseCol}" \\
  --type-col "${powerAnalysisTypeCol}" \\
  --sample-sizes-cancer "${powerAnalysisSampleSizesCancer}" \\
  --sample-sizes-stype "${powerAnalysisSampleSizesStype}" \\
  --n-simulations ${powerAnalysisNSimulations} \\
  --n-perm ${powerAnalysisNPerm} \\
  --alpha ${powerAnalysisAlpha} \\
  --seed ${powerAnalysisSeed} \\
  --transform "${powerAnalysisTransform}" \\
  --contralateral-sample-types "${powerAnalysisContralateralTypes}" \\
  "\${POWER_SKIP_ARGS[@]}" \\
  "\${POWER_CONTRALATERAL_ARGS[@]}" \\
  "\${POWER_INDICSPECIES_ARGS[@]}"

touch power_analysis.done
"""
}

process TAXONOMY_GROUP_ASSOCIATION {
    cpus pipelineThreads
    conda "${taxonomyPatientAwareCondaEnvPath}"

    input:
    path(asv_meta)
    path(asv_counts)

    output:
    path("taxonomy_patient_aware.done"), emit: done

    when:
    taxonomyPatientAwareEnabled

    script:
    def excludeContralateralFlag = taxonomyPatientAwareExcludeContralateral ? '1' : '0'
    def skipOmnibusFlag = taxonomyPatientAwareSkipOmnibus ? '1' : '0'
    def runComparisonFlag = taxonomyPatientAwareRunComparison ? '1' : '0'
    """
set -euo pipefail
mkdir -p "${taxonomyPatientAwareOutputDirAbs}"

SUMMARY_INPUT_DIR="taxonomy_patient_aware_inputs"
mkdir -p "\${SUMMARY_INPUT_DIR}"

python "${masterSummaryScriptPath}" \\
  --data-dir "${outputDir}" \\
  --asv-meta "${asv_meta}" \\
  --asv-counts "${asv_counts}" \\
  --clustermaps-dir "${clustermapsOutputDirAbs}" \\
  --indicspecies-dir "${indicspeciesOutputDirAbs}" \\
  --spieceasi-dir "${spieceasiOutputDirAbs}" \\
  --outdir "\${SUMMARY_INPUT_DIR}" \\
  --max-direct-cols ${masterSummaryMaxDirectCols}

if [[ ! -f "\${SUMMARY_INPUT_DIR}/ASV_master_long.tsv" ]]; then
  echo "Missing expected taxonomy patient-aware input: \${SUMMARY_INPUT_DIR}/ASV_master_long.tsv" >&2
  exit 1
fi

TAXONOMY_CONTRALATERAL_ARGS=()
if [[ "${excludeContralateralFlag}" == "1" ]]; then
  TAXONOMY_CONTRALATERAL_ARGS+=(--exclude-contralateral-in-cancer)
else
  TAXONOMY_CONTRALATERAL_ARGS+=(--keep-contralateral-in-cancer)
fi

TAXONOMY_OMNIBUS_ARGS=()
if [[ "${skipOmnibusFlag}" == "1" ]]; then
  TAXONOMY_OMNIBUS_ARGS+=(--skip-omnibus)
fi

CANCER_OUTDIR="${taxonomyPatientAwareOutputDirAbs}/cancer_vs_control"
SAMPLETYPE_OUTDIR="${taxonomyPatientAwareOutputDirAbs}/sample_type"
FIGURES_OUTDIR="${taxonomyPatientAwareOutputDirAbs}/figures"
mkdir -p "\${CANCER_OUTDIR}" "\${SAMPLETYPE_OUTDIR}" "\${FIGURES_OUTDIR}"

if [[ "${runComparisonFlag}" == "1" ]]; then
  python "${taxonomicAbundanceObservedScriptPath}" \\
    --data-long "\${SUMMARY_INPUT_DIR}/ASV_master_long.tsv" \\
    --tax-levels "${taxonomyPatientAwareTaxLevels}" \\
    --sample-types "${taxonomyPatientAwareSampleTypes}" \\
    --case-groups "${taxonomyPatientAwareComparisonGroups}" \\
    --sample-col "${taxonomyPatientAwareSampleCol}" \\
    --patient-col "${taxonomyPatientAwarePatientCol}" \\
    --case-col "${taxonomyPatientAwareCaseCol}" \\
    --type-col "${taxonomyPatientAwareTypeCol}" \\
    --count-col "${taxonomyPatientAwareCountCol}" \\
    --min-prevalence ${taxonomyPatientAwareMinPrevalence} \\
    --contralateral-col "${taxonomyPatientAwareContralateralCol}" \\
    --cancer-site-col "${taxonomyPatientAwareCancerSiteCol}" \\
    --lung-side-col "${taxonomyPatientAwareLungSideCol}" \\
    --contralateral-value "${taxonomyPatientAwareContralateralValue}" \\
    --contralateral-sample-types "${taxonomyPatientAwareContralateralTypes}" \\
    --transform "${taxonomyPatientAwareTransform}" \\
    --outdir "\${CANCER_OUTDIR}" \\
    "\${TAXONOMY_CONTRALATERAL_ARGS[@]}"
else
  printf 'tax_level\\tsample_type\\ttaxon\\tgroup_a\\tgroup_b\\tn_patients_total\\tn_group_a\\tn_group_b\\tn_cancer\\tn_control\\tmedian_group_a\\tmedian_group_b\\tmedian_cancer\\tmedian_control\\tdelta_median\\tcohens_d\\tmw_u\\tp_value\\tq_value\\tsignificant_fdr_0.05\\n' > "\${CANCER_OUTDIR}/taxonomic_abundance_observed.tsv"
  cp "\${CANCER_OUTDIR}/taxonomic_abundance_observed.tsv" "\${CANCER_OUTDIR}/taxonomic_abundance_observed_significant.tsv"
fi

python "${taxonomicSampleTypeObservedScriptPath}" \\
  --data-long "\${SUMMARY_INPUT_DIR}/ASV_master_long.tsv" \\
  --tax-levels "${taxonomyPatientAwareTaxLevels}" \\
  --sample-types "${taxonomyPatientAwareSampleTypes}" \\
  --sample-col "${taxonomyPatientAwareSampleCol}" \\
  --patient-col "${taxonomyPatientAwarePatientCol}" \\
  --case-col "${taxonomyPatientAwareCaseCol}" \\
  --type-col "${taxonomyPatientAwareTypeCol}" \\
  --count-col "${taxonomyPatientAwareCountCol}" \\
  --min-prevalence ${taxonomyPatientAwareMinPrevalence} \\
  --contralateral-col "${taxonomyPatientAwareContralateralCol}" \\
  --cancer-site-col "${taxonomyPatientAwareCancerSiteCol}" \\
  --lung-side-col "${taxonomyPatientAwareLungSideCol}" \\
  --contralateral-value "${taxonomyPatientAwareContralateralValue}" \\
  --contralateral-sample-types "${taxonomyPatientAwareContralateralTypes}" \\
  --transform "${taxonomyPatientAwareTransform}" \\
  --outdir "\${SAMPLETYPE_OUTDIR}" \\
  "\${TAXONOMY_CONTRALATERAL_ARGS[@]}" \\
  "\${TAXONOMY_OMNIBUS_ARGS[@]}"

python "${plotTaxonomicObservedScriptPath}" \\
  --data-long "\${SUMMARY_INPUT_DIR}/ASV_master_long.tsv" \\
  --cancer-results "\${CANCER_OUTDIR}/taxonomic_abundance_observed.tsv" \\
  --sampletype-results "\${SAMPLETYPE_OUTDIR}/taxonomic_sample_type_observed_pairwise.tsv" \\
  --outdir "\${FIGURES_OUTDIR}" \\
  --alpha ${taxonomyPatientAwareAlpha} \\
  --top-n ${taxonomyPatientAwareTopN} \\
  --sample-col "${taxonomyPatientAwareSampleCol}" \\
  --patient-col "${taxonomyPatientAwarePatientCol}" \\
  --type-col "${taxonomyPatientAwareTypeCol}" \\
  --case-col "${taxonomyPatientAwareCaseCol}" \\
  --count-col "${taxonomyPatientAwareCountCol}" \\
  --type-palette "${taxonomyPatientAwareTypePalette}" \\
  --case-palette "${taxonomyPatientAwareCasePalette}"

touch taxonomy_patient_aware.done
"""
}

process PAIRED_GROUP_CONTRAST {
    cpus pipelineThreads
    conda "${lungStatusAnalysisCondaEnvPath}"

    input:
    path(asv_meta)
    path(asv_counts)

    output:
    path("lung_status_analysis.done"), emit: done

    when:
    lungStatusAnalysisEnabled

    script:
    """
set -euo pipefail
mkdir -p "${lungStatusAnalysisOutputDirAbs}"

SUMMARY_INPUT_DIR="lung_status_analysis_inputs"
mkdir -p "\${SUMMARY_INPUT_DIR}"

python "${masterSummaryScriptPath}" \\
  --data-dir "${outputDir}" \\
  --asv-meta "${asv_meta}" \\
  --asv-counts "${asv_counts}" \\
  --clustermaps-dir "${clustermapsOutputDirAbs}" \\
  --indicspecies-dir "${indicspeciesOutputDirAbs}" \\
  --spieceasi-dir "${spieceasiOutputDirAbs}" \\
  --outdir "\${SUMMARY_INPUT_DIR}" \\
  --max-direct-cols ${masterSummaryMaxDirectCols}

if [[ ! -f "\${SUMMARY_INPUT_DIR}/ASV_master_long.tsv" ]]; then
  echo "Missing expected lung-status input: \${SUMMARY_INPUT_DIR}/ASV_master_long.tsv" >&2
  exit 1
fi

IFS=',' read -r -a LUNG_SAMPLE_TYPES <<< "${lungStatusAnalysisSampleTypes}"
for sample_type in "\${LUNG_SAMPLE_TYPES[@]}"; do
  sample_type="\$(printf '%s' "\${sample_type}" | xargs)"
  [[ -n "\${sample_type}" ]] || continue
  sample_slug="\${sample_type// /_}"
  sample_root="${lungStatusAnalysisOutputDirAbs}/\${sample_slug}"
  data_dir="\${sample_root}/data"
  results_dir="\${sample_root}/results"
  figures_dir="\${sample_root}/figures"
  mkdir -p "\${data_dir}" "\${results_dir}" "\${figures_dir}"

  python "${prepareLungStatusScriptPath}" \\
    --input "\${SUMMARY_INPUT_DIR}/ASV_master_long.tsv" \\
    --sample-type "\${sample_type}" \\
    --sample-col "${lungStatusAnalysisSampleCol}" \\
    --type-col "${lungStatusAnalysisTypeCol}" \\
    --case-col "${lungStatusAnalysisCaseCol}" \\
    --patient-col "${lungStatusAnalysisPatientCol}" \\
    --cancer-site-col "${lungStatusAnalysisCancerSiteCol}" \\
    --lung-code-col "${lungStatusAnalysisLungCodeCol}" \\
    --tumor-side-col "${lungStatusAnalysisTumorSideCol}" \\
    --contralateral-col "${lungStatusAnalysisContralateralCol}" \\
    --healthy-col "${lungStatusAnalysisHealthyCol}" \\
    --lung-status-col "${lungStatusAnalysisStatusCol}" \\
    --status-a-value "${lungStatusAnalysisStatusAValue}" \\
    --status-b-value "${lungStatusAnalysisStatusBValue}" \\
    --reference-status-value "${lungStatusAnalysisReferenceStatusValue}" \\
    --outdir "\${data_dir}"

  meta_file="\$(find "\${data_dir}" -maxdepth 1 -name '*_metadata.tsv' | head -n 1)"
  asv_file="\$(find "\${data_dir}" -maxdepth 1 -name '*_ASV_table.tsv' | head -n 1)"
  if [[ -z "\${meta_file}" || -z "\${asv_file}" ]]; then
    echo "Missing prepared lung-status inputs for sample type: \${sample_type}" >&2
    exit 1
  fi

  Rscript "${lungStatusAnalysisScriptPath}" "\${meta_file}" "\${asv_file}" "\${results_dir}" \
    ${lungStatusAnalysisPermutations} ${lungStatusAnalysisSeed}

  python "${plotLungStatusScriptPath}" \\
    --metadata "\${meta_file}" \\
    --patient-level "\${results_dir}/patient_level_metadata.tsv" \\
    --distances "\${results_dir}/patient_level_bray_distances.tsv" \\
    --summary "\${results_dir}/lung_status_contrasts_summary.tsv" \\
    --pairdist-a "\${results_dir}/contrast_A_pairwise_distances.tsv" \\
    --asv-table "\${asv_file}" \\
    --outdir "\${figures_dir}"
done

touch lung_status_analysis.done
"""
}

process CLUSTERMAPS {
    cpus pipelineThreads
    conda "${clustermapsCondaEnvPath}"

    input:
    path(asv_meta)
    path(metadata_table)
    val(indicspecies_ready)

    output:
    path("clustermaps.done"), emit: done

    when:
    clustermapsEnabled

    script:
    def group3ColArg = clustermapsGroup3Col ? """  --group3-col "${clustermapsGroup3Col}" \\\n""" : ''
    def excludeGroup1Arg = clustermapsExcludeGroup1 ? """  --exclude-group1 "${clustermapsExcludeGroup1}" \\\n""" : ''
    def group1PaletteArg = clustermapsGroup1Palette ? """  --group1-palette "${clustermapsGroup1Palette}" \\\n""" : ''
    def group2PaletteArg = clustermapsGroup2Palette ? """  --group2-palette "${clustermapsGroup2Palette}" \\\n""" : ''
    def group3PaletteArg = clustermapsGroup3Palette ? """  --group3-palette "${clustermapsGroup3Palette}" \\\n""" : ''
    def clustermapsGroup1OrderArg = clustermapsGroup1Order && !clustermapsGroup1Order.isEmpty() ? """  --group1-order "${clustermapsGroup1Order.join(',')}" \\\n""" : ''
    def clustermapsRunMitoFlag = clustermapsRunMito ? '1' : '0'
    def isaFileArg = clustermapsIsaFile ?: ''
    def isaSearchDirArg = clustermapsIsaSearchDir ?: ''
    def indicspeciesReadyFlag = indicspeciesEnabled ? '1' : '0'
    def isaAutoCandidatesCsv = clustermapsIsaAutoCandidates.join(',')
    def isaSigColsArg = clustermapsIsaSignificanceCols ? """  --isa-significance-cols "${clustermapsIsaSignificanceCols}" \\\n""" : ''
    def isaStatColsArg = clustermapsIsaStatCols ? """  --isa-stat-cols "${clustermapsIsaStatCols}" \\\n""" : ''
    def clustermapsFormatsArg = clustermapsFormats ? """  --formats "${clustermapsFormats}" \\\n""" : ''
    def clustermapsFigWidthArg = clustermapsFigWidth != null ? """  --figwidth ${clustermapsFigWidth} \\\n""" : ''
    def clustermapsRowHeightArg = clustermapsRowHeight != null ? """  --row-height ${clustermapsRowHeight} \\\n""" : ''
    def clustermapsMinHeightArg = clustermapsMinHeight != null ? """  --min-height ${clustermapsMinHeight} \\\n""" : ''
    def clustermapsMaxHeightArg = clustermapsMaxHeight != null ? """  --max-height ${clustermapsMaxHeight} \\\n""" : ''
    """
set -euo pipefail
mkdir -p "${clustermapsOutputDirAbs}"
mkdir -p "${clustermapsMitoOutputDirAbs}"

ISA_ARGS=()
ISA_RESOLVED=""
ISA_SOURCE_DIR=""

if [[ -n "${isaFileArg}" ]]; then
  if [[ -f "${isaFileArg}" ]]; then
    ISA_RESOLVED="${isaFileArg}"
  elif [[ -d "${isaFileArg}" ]]; then
    ISA_SOURCE_DIR="${isaFileArg}"
  else
    echo "[w] clustermaps.isa_file path not found; skipping ISA gate: ${isaFileArg}" >&2
  fi
fi

if [[ -z "\${ISA_SOURCE_DIR}" && -n "${isaSearchDirArg}" && -d "${isaSearchDirArg}" ]]; then
  ISA_SOURCE_DIR="${isaSearchDirArg}"
fi

if [[ -z "\${ISA_SOURCE_DIR}" && "${indicspeciesReadyFlag}" == "1" && -d "${indicspeciesOutputDirAbs}" ]]; then
  ISA_SOURCE_DIR="${indicspeciesOutputDirAbs}"
fi

if [[ -z "\${ISA_RESOLVED}" && -n "\${ISA_SOURCE_DIR}" ]]; then
  IFS=',' read -r -a ISA_CANDIDATES <<< "${isaAutoCandidatesCsv}"
  for isa_name in "\${ISA_CANDIDATES[@]}"; do
    [[ -n "\${isa_name}" ]] || continue
    if [[ -f "\${ISA_SOURCE_DIR}/\${isa_name}" ]]; then
      ISA_RESOLVED="\${ISA_SOURCE_DIR}/\${isa_name}"
      break
    fi
  done

  if [[ -z "\${ISA_RESOLVED}" ]]; then
    first_summary=\$(find "\${ISA_SOURCE_DIR}" -maxdepth 1 -type f -name '*_indicator_species*_summary.tsv' | sort | head -n 1 || true)
    if [[ -n "\${first_summary}" ]]; then
      ISA_RESOLVED="\${first_summary}"
    fi
  fi
fi

if [[ -n "\${ISA_RESOLVED}" ]]; then
  echo "[i] CLUSTERMAPS using ISA table: \${ISA_RESOLVED}" >&2
  ISA_ARGS=(--isa "\${ISA_RESOLVED}")
fi

if [[ "${clustermapsRunMitoFlag}" == "1" && -f "${clustermapsMitoInputPath}" ]]; then
  python "${clustermapsScriptPath}" \\
    --asv-meta "${asv_meta}" \\
    --metadata "${metadata_table}" \\
    --outdir "${clustermapsOutputDirAbs}" \\
    --sample-col "${clustermapsSampleCol}" \\
    --sample-code-col "${clustermapsSampleCodeCol}" \\
    --asv-id-col "${clustermapsAsvIdCol}" \\
    --group1-col "${clustermapsGroup1Col}" \\
    --group2-col "${clustermapsGroup2Col}" \\
${group3ColArg}${clustermapsGroup1OrderArg}${excludeGroup1Arg}${group1PaletteArg}${group2PaletteArg}\
${group3PaletteArg}    --ranks "${clustermapsRanks}" \\
    --topN "${clustermapsTopN}" \\
    --count-col "${clustermapsCountCol}" \\
    --isa-min-stat ${clustermapsIsaMinStat} \\
${clustermapsFormatsArg}${clustermapsFigWidthArg}${clustermapsRowHeightArg}${clustermapsMinHeightArg}${clustermapsMaxHeightArg}\
${isaSigColsArg}${isaStatColsArg}    --mito-sample-mode "${clustermapsMitoSampleMode}" \\
    --mito-asv "${clustermapsMitoInputPath}" \\
    --mito-outdir "${clustermapsMitoOutputDirAbs}" \\
    \${ISA_ARGS[@]}
else
  python "${clustermapsScriptPath}" \\
    --asv-meta "${asv_meta}" \\
    --metadata "${metadata_table}" \\
    --outdir "${clustermapsOutputDirAbs}" \\
    --sample-col "${clustermapsSampleCol}" \\
    --sample-code-col "${clustermapsSampleCodeCol}" \\
    --asv-id-col "${clustermapsAsvIdCol}" \\
    --group1-col "${clustermapsGroup1Col}" \\
    --group2-col "${clustermapsGroup2Col}" \\
${group3ColArg}${clustermapsGroup1OrderArg}${excludeGroup1Arg}${group1PaletteArg}${group2PaletteArg}\
${group3PaletteArg}    --ranks "${clustermapsRanks}" \\
    --topN "${clustermapsTopN}" \\
    --count-col "${clustermapsCountCol}" \\
    --isa-min-stat ${clustermapsIsaMinStat} \\
${clustermapsFormatsArg}${clustermapsFigWidthArg}${clustermapsRowHeightArg}${clustermapsMinHeightArg}${clustermapsMaxHeightArg}\
${isaSigColsArg}${isaStatColsArg}    --mito-sample-mode "${clustermapsMitoSampleMode}" \\
    \${ISA_ARGS[@]}
fi

touch clustermaps.done
"""
}

process SPIECEASI {
    cpus pipelineThreads
    conda "${spieceasiCondaEnvPath}"

    input:
    path(asv_counts)
    path(force_keep_asvs)

    output:
    path("spieceasi_network_pos_all.graphml"), emit: graph_all
    path("spieceasi_network_pos_thr.graphml"), emit: graph_thr
    path("spieceasi_node_features.csv"), emit: node_features
    path("spieceasi.done"), emit: done

    when:
    spieceasiEnabled

    script:
    def transposeFlag = spieceasiTranspose ? 'TRUE' : 'FALSE'
    def removeZeroVarFlag = spieceasiRemoveZeroVar ? 'TRUE' : 'FALSE'
    def keepNegativeFlag = spieceasiKeepNegative ? 'TRUE' : 'FALSE'
    def forceFilterFlag = spieceasiForceFilter ? 'TRUE' : 'FALSE'
    def forceSpieceasiFlag = spieceasiForceSpieceasi ? 'TRUE' : 'FALSE'
    def forceGraphsFlag = spieceasiForceGraphs ? 'TRUE' : 'FALSE'
    def forceKeepAsvsArg = indicspeciesEnabled ? """  --force-keep-asvs "${force_keep_asvs}" \\\n""" : ''
    """
set -euo pipefail
mkdir -p "${spieceasiOutputDirAbs}"

Rscript "${spieceasiScriptPath}" \\
  --counts "${asv_counts}" \\
  --outdir "${spieceasiOutputDirAbs}" \\
  --prefix "${spieceasiPrefix}" \\
  --transpose ${transposeFlag} \\
  --min-rel-abund ${spieceasiMinRelAbund} \\
  --min-prevalence ${spieceasiMinPrevalence} \\
${forceKeepAsvsArg}  --remove-zero-var ${removeZeroVarFlag} \\
  --method "${spieceasiMethod}" \\
  --lambda-min-ratio ${spieceasiLambdaMinRatio} \\
  --nlambda ${spieceasiNlambda} \\
  --rep-num ${spieceasiRepNum} \\
  --thresh ${spieceasiThresh} \\
  --pulsar-criterion "${spieceasiPulsarCriterion}" \\
  --ncores ${spieceasiNcores} \\
  --seed ${spieceasiSeed} \\
  --edge-threshold ${spieceasiEdgeThreshold} \\
  --keep-negative ${keepNegativeFlag} \\
  --layout-iters ${spieceasiLayoutIters} \\
  --force-filter ${forceFilterFlag} \\
  --force-spieceasi ${forceSpieceasiFlag} \\
  --force-graphs ${forceGraphsFlag}

GRAPH_ALL="${spieceasiOutputDirAbs}/${spieceasiPrefix}_network_pos_all.graphml"
GRAPH_THR="${spieceasiOutputDirAbs}/${spieceasiPrefix}_network_pos_thr.graphml"
NODE_FEATURES="${spieceasiOutputDirAbs}/${spieceasiPrefix}_node_features.csv"

for f in "\${GRAPH_ALL}" "\${GRAPH_THR}" "\${NODE_FEATURES}"; do
  if [[ ! -f "\${f}" ]]; then
    echo "Missing expected SPIEC-EASI output: \${f}" >&2
    exit 1
  fi
done

ln -sf "\${GRAPH_ALL}" spieceasi_network_pos_all.graphml
ln -sf "\${GRAPH_THR}" spieceasi_network_pos_thr.graphml
ln -sf "\${NODE_FEATURES}" spieceasi_node_features.csv
touch spieceasi.done
"""
}

process NETWORK_MODULES {
    cpus pipelineThreads
    conda "${networkModulesCondaEnvPath}"

    input:
    path(graph_all, stageAs: 'network_graph_all.graphml')
    path(graph_thr, stageAs: 'network_graph_sub.graphml')

    output:
    path("network_modules_sub.tsv"), emit: modules_sub
    path("network_modules_all.tsv"), emit: modules_all
    path("network_modules_summary.tsv"), emit: summary
    path("network_modules_runs.tsv"), emit: runs
    path("network_modules.done"), emit: done

    when:
    networkEnabled && networkModulesEnabled

    script:
    def methodsCsv = networkModuleMethods.join(',')
    def resolutionsCsv = networkModuleResolutions.join(',')
    """
set -euo pipefail
mkdir -p "${spieceasiOutputDirAbs}"

Rscript "${networkModulesScriptPath}" \\
  --graph-sub "${graph_thr}" \\
  --graph-all "${graph_all}" \\
  --outdir "${spieceasiOutputDirAbs}" \\
  --prefix "${spieceasiPrefix}" \\
  --methods "${methodsCsv}" \\
  --primary-method "${networkModulePrimaryMethod}" \\
  --reps ${networkModuleReps} \\
  --resolutions "${resolutionsCsv}" \\
  --consensus-threshold ${networkModuleConsensusThreshold} \\
  --seed ${networkModuleSeed}

MODULES_SUB="${spieceasiOutputDirAbs}/${spieceasiPrefix}_modules_sub.tsv"
MODULES_ALL="${spieceasiOutputDirAbs}/${spieceasiPrefix}_modules_all.tsv"
MODULE_SUMMARY="${spieceasiOutputDirAbs}/${spieceasiPrefix}_module_summary.tsv"
MODULE_RUNS="${spieceasiOutputDirAbs}/${spieceasiPrefix}_module_runs.tsv"

for f in "\${MODULES_SUB}" "\${MODULES_ALL}" "\${MODULE_SUMMARY}" "\${MODULE_RUNS}"; do
  if [[ ! -f "\${f}" ]]; then
    echo "Missing expected network module output: \${f}" >&2
    exit 1
  fi
done

ln -sf "\${MODULES_SUB}" network_modules_sub.tsv
ln -sf "\${MODULES_ALL}" network_modules_all.tsv
ln -sf "\${MODULE_SUMMARY}" network_modules_summary.tsv
ln -sf "\${MODULE_RUNS}" network_modules_runs.tsv
touch network_modules.done
"""
}

process GRAPH_NETWORK {
    cpus pipelineThreads
    conda "${networkCondaEnvPath}"

    input:
    path(graph_all, stageAs: 'network_graph_all.graphml')
    path(graph_thr, stageAs: 'network_graph_sub.graphml')
    path(node_features)
    path(asv_counts)
    path(metadata_table)
    path(dep_asv_mag, stageAs: 'dep_asv_mag.tsv')
    path(taxonomy_table)
    path(indicspecies_tables)
    path(modules_sub, stageAs: 'network_modules_sub.tsv')
    path(modules_all, stageAs: 'network_modules_all.tsv')

    output:
    path("network.done"), emit: done

    when:
    networkEnabled

    script:
    def networkModesArg = networkModes && !networkModes.isEmpty() ? """  --modes ${networkModes.collect { "\"${it}\"" }.join(' ')} \\\n""" : ''
    def networkModuleBestOnlyArg = networkModuleBestOnly ? """  --module-best-only \\\n""" : ''
    def networkModuleIsaOnlyArg = networkModuleIsaOnly ? """  --module-isa-only \\\n""" : ''
    def networkModuleColorByIsaArg = networkModuleColorByIsa ? """  --module-color-by-isa \\\n""" : ''
    def asvMagPairingArg = asvMagLinkEnabled ? """  --asv-mag-pairing "${asvMagLinkOutputDirAbs}/tables/asv2mag_pairing.tsv" \\\n""" : ''
    def isaSummaryModeArg = indicspeciesUseDuleg ? 'duleg' : 'default'
    """
set -euo pipefail
mkdir -p "${spieceasiOutputDirAbs}"

python "${graphNetworkScriptPath}" \\
  --data-dir "${outputDir}" \\
  --outdir "${spieceasiOutputDirAbs}" \\
  --graph-pos-all "${graph_all}" \\
  --graph-pos-sub "${graph_thr}" \\
  --node-features "${node_features}" \\
  --asv-counts "${asv_counts}" \\
  --taxonomy "${taxonomy_table}" \\
  --metadata "${metadata_table}" \\
  --sample-col "${indicspeciesSampleCol}" \\
  --isa-group-cols "${networkIsaOverlayGroupsCsv}" \\
  --isa-summary-mode "${isaSummaryModeArg}" \\
  --isa-palette-map-json '${networkGroupPaletteJson}' \\
  --isa-order-map-json '${networkGroupOrderJson}' \\
  --isa-focus-map-json '${networkFocusLabelJson}' \\
${asvMagPairingArg}\
  --color-col "${networkColorCol}" \\
${networkModuleBestOnlyArg}${networkModuleIsaOnlyArg}${networkModuleColorByIsaArg}  --module-best-min-size ${networkModuleBestMinSize} \\
  --module-best-min-stability ${networkModuleBestMinStability} \\
  --module-isa-source "${networkModuleIsaSource}" \\
  --module-isa-min-stat ${networkModuleIsaMinStat} \\
  --module-isa-max-q ${networkModuleIsaMaxQ} \\
  --modules-sub "${modules_sub}" \\
  --modules-all "${modules_all}" \\
${networkModesArg}  --layout-seed ${networkLayoutSeed} \\
  --layout-scale ${networkLayoutScale} \\
  --degree-scale ${networkDegreeScale} \\
  --degree-size-mode "${networkDegreeSizeMode}" \\
  --degree-min-area ${networkDegreeMinArea} \\
  --edge-width-scale ${networkEdgeWidthScale} \\
  --isa-scale ${networkIsaScale} \\
  --abundance-size-mode "${networkAbundanceSizeMode}" \\
  --abundance-reference ${networkAbundanceReference} \\
  --abundance-reference-area ${networkAbundanceReferenceArea} \\
  --abundance-min-area ${networkAbundanceMinArea} \\
  --abundance-max-area ${networkAbundanceMaxArea} \\
  --abundance-scale-power ${networkAbundanceScalePower}

touch network.done
"""
}

process ASV_MAG_NETWORK {
    cpus 1
    conda "${asvMagNetworkCondaEnvPath}"

    input:
    path(graph, stageAs: 'asv_mag_network_graph.graphml')
    path(node_features)
    path(taxonomy_table)
    path(asv_counts)
    path(dep_asv_mag, stageAs: 'dep_asv_mag.done')

    output:
    path("asv_mag_network.done"), emit: done

    when:
    asvMagNetworkEnabled

    script:
    def magAbundanceArg = asvMagNetworkMagAbundance ? """  --mag-abundance "${asvMagNetworkMagAbundance}" \\\n""" : ''
    def functionalArgs = asvMagNetworkFunctionalAnnotations ? asvMagNetworkFunctionalAnnotations.collect { """  --functional-annotation "${it}" \\\n""" }.join('') : ''
    """
set -euo pipefail
mkdir -p "${asvMagNetworkOutputDirAbs}"

echo "asv_mag_network.py md5: ${asvMagNetworkScriptHash}"
python "${asvMagNetworkScriptPath}" \\
  --graph "${graph}" \\
  --node-features "${node_features}" \\
  --asv-mag-pairing "${asvMagLinkOutputDirAbs}/tables/asv2mag_pairing.tsv" \\
  --taxonomy "${taxonomy_table}" \\
  --asv-counts "${asv_counts}" \\
  --genome-summary "${asvMagLinkOutputDirAbs}/tables/asv2mag_genome_summary.tsv" \\
  --reference-catalog "${asvMagLinkOutputDirAbs}/references/barrnap_16s_reference_catalog.tsv" \\
  --outdir "${asvMagNetworkOutputDirAbs}" \\
  --prefix "${asvMagNetworkPrefix}" \\
  --asv-taxonomy-source "${asvMagNetworkAsvTaxonomySource}" \\
  --mag-taxonomy-source "${asvMagNetworkMagTaxonomySource}" \\
  --mag-id-mode "${asvMagNetworkMagIdMode}" \\
  --mag-abundance-format "${asvMagNetworkMagAbundanceFormat}" \\
  --mag-abundance-genome-col "${asvMagNetworkMagAbundanceGenomeCol}" \\
  --mag-abundance-sample-col "${asvMagNetworkMagAbundanceSampleCol}" \\
  --mag-abundance-value-col "${asvMagNetworkMagAbundanceValueCol}" \\
  --min-shared-samples ${asvMagNetworkMinSharedSamples} \\
  --abundance-transform "${asvMagNetworkAbundanceTransform}" \\
  --functional-module-min-fraction ${asvMagNetworkFunctionalModuleMinFraction} \\
  --min-pident ${asvMagNetworkMinPident} \\
${magAbundanceArg}${functionalArgs}  --min-qcov ${asvMagNetworkMinQcov}
touch asv_mag_network.done
"""
}

process MODULE_MAG_ANCHORS {
    cpus 1
    conda "${networkCondaEnvPath}"

    input:
    path(modules_all)
    path(node_features)
    path(taxonomy_table)
    path(asv_counts)
    path(metadata_table)
    path(dep_asv_mag)
    path(dep_graph_network)

    output:
    path("module_asv_anchor_table.tsv"), emit: asv_anchor_table
    path("module_mag_anchor_summary.tsv"), emit: module_summary
    path("sample_module_scores.tsv"), emit: sample_module_scores
    path("sample_top_modules.tsv"), emit: sample_top_modules
    path("sample_module_score_matrix.tsv"), emit: sample_module_matrix
    path("sample_module_score_heatmap.png"), optional: true, emit: sample_module_heatmap_png
    path("sample_module_score_heatmap.pdf"), optional: true, emit: sample_module_heatmap_pdf
    path("sample_module_score_heatmap.svg"), optional: true, emit: sample_module_heatmap_svg
    path("module_mag_anchors.done"), emit: done

    when:
    networkEnabled && asvMagLinkEnabled

    script:
    """
set -euo pipefail
mkdir -p "${spieceasiOutputDirAbs}"

python "${moduleMagAnchorsScriptPath}" \\
  --modules "${modules_all}" \\
  --node-features "${node_features}" \\
  --taxonomy "${taxonomy_table}" \\
  --asv-mag-pairing "${asvMagLinkOutputDirAbs}/tables/asv2mag_pairing.tsv" \\
  --asv-counts "${asv_counts}" \\
  --metadata "${metadata_table}" \\
  --sample-col "${metadataPlotsSampleCol}" \\
  --sample-code-col "${clustermapsSampleCodeCol}" \\
  --best-stats "${spieceasiOutputDirAbs}/network_modules_best_stats_all.tsv" \\
  --outdir "${spieceasiOutputDirAbs}"

ln -sf "${spieceasiOutputDirAbs}/module_asv_anchor_table.tsv" module_asv_anchor_table.tsv
ln -sf "${spieceasiOutputDirAbs}/module_mag_anchor_summary.tsv" module_mag_anchor_summary.tsv
ln -sf "${spieceasiOutputDirAbs}/sample_module_scores.tsv" sample_module_scores.tsv
ln -sf "${spieceasiOutputDirAbs}/sample_top_modules.tsv" sample_top_modules.tsv
ln -sf "${spieceasiOutputDirAbs}/sample_module_score_matrix.tsv" sample_module_score_matrix.tsv
for ext in png pdf svg; do
  if [[ -f "${spieceasiOutputDirAbs}/sample_module_score_heatmap.\${ext}" ]]; then
    ln -sf "${spieceasiOutputDirAbs}/sample_module_score_heatmap.\${ext}" "sample_module_score_heatmap.\${ext}"
  fi
done
touch module_mag_anchors.done
"""
}

process MASTER_SUMMARY {
    cpus 1
    conda "${masterSummaryCondaEnvPath}"

    input:
    path(asv_meta)
    path(asv_counts)
    path(dep_network, stageAs: 'dep_network.done')
    path(dep_sankey, stageAs: 'dep_sankey.done')
    path(dep_asv_mag, stageAs: 'dep_asv_mag.done')
    path(dep_optional, stageAs: 'dep_optional.done')

    output:
    path("ASV_master_long.tsv"), optional: true, emit: master_long
    path("ASV_master_count_wide.tsv"), optional: true, emit: master_count
    path("ASV_master_source_manifest.tsv"), optional: true, emit: master_manifest
    path("ASV_master_column_mapping.tsv"), optional: true, emit: master_colmap
    path("ASV_master_column_collisions_original.tsv"), optional: true, emit: master_collisions
    path("master_summary.done"), emit: done

    when:
    masterSummaryEnabled

    script:
    def whitelistArg = masterSummaryWhitelistCsv ? """  --whitelist "${masterSummaryWhitelistCsv}" \\\n""" : ''
    """
set -euo pipefail
mkdir -p "${masterSummaryOutputDirAbs}"

python "${masterSummaryScriptPath}" \\
  --data-dir "${outputDir}" \\
  --asv-meta "${asv_meta}" \\
  --asv-counts "${asv_counts}" \\
  --clustermaps-dir "${masterSummaryClustermapsDirAbs}" \\
  --indicspecies-dir "${masterSummaryIndicspeciesDirAbs}" \\
  --spieceasi-dir "${masterSummarySpieceasiDirAbs}" \\
  --asv-mag-dir "${masterSummaryAsvMagDirAbs}" \\
${whitelistArg}  --outdir "${masterSummaryOutputDirAbs}" \\
  --max-direct-cols ${masterSummaryMaxDirectCols}

link_if_exists() {
  local src="\$1"
  local dest="\$2"
  if [[ -f "\${src}" ]]; then
    ln -sf "\${src}" "\${dest}"
  fi
}

link_if_exists "${masterSummaryOutputDirAbs}/ASV_master_long.tsv" "ASV_master_long.tsv"
link_if_exists "${masterSummaryOutputDirAbs}/ASV_master_count_wide.tsv" "ASV_master_count_wide.tsv"
link_if_exists "${masterSummaryOutputDirAbs}/ASV_master_source_manifest.tsv" "ASV_master_source_manifest.tsv"
link_if_exists "${masterSummaryOutputDirAbs}/ASV_master_column_mapping.tsv" "ASV_master_column_mapping.tsv"
link_if_exists "${masterSummaryOutputDirAbs}/ASV_master_column_collisions_original.tsv" "ASV_master_column_collisions_original.tsv"

touch master_summary.done
"""
}

process ASV_MAG_LINK {
    cpus asvMagLinkThreads
    conda "${asvMagLinkCondaEnvPath}"

    input:
    path(filtered_fasta)

    output:
    path("asv_mag_link.done"), emit: done

    when:
    asvMagLinkEnabled

    script:
    def masterTsvArg = asvMagLinkMasterTsv ? """  --master-tsv "${asvMagLinkMasterTsv}" \\\n""" : ''
    def genomeDirArg = asvMagLinkGenomeDir ? """  --genome-fasta-dir "${asvMagLinkGenomeDir}" \\\n""" : ''
    def genomeQcDirArg = asvMagLinkGenomeQcDir ? """  --genome-qc-dir "${asvMagLinkGenomeQcDir}" \\\n""" : ''
    def genomeQcDirsArg = asvMagLinkGenomeQcDirs ? asvMagLinkGenomeQcDirs.collect { """  --genome-qc-dir "${it}" \\\n""" }.join('') : ''
    def idTokenIndexesArg = asvMagLinkIdTokenIndexes ? asvMagLinkIdTokenIndexes.collect { """  --id-token-index ${it} \\\n""" }.join('') : ''
    def barrnapDirArg = asvMagLinkBarrnapDir ? """  --barrnap-dir "${asvMagLinkBarrnapDir}" \\\n""" : ''
    """
set -euo pipefail
mkdir -p "${asvMagLinkOutputDirAbs}"

python "${asvMagLinkScriptPath}" \\
  --asv-fasta "${filtered_fasta}" \\
${masterTsvArg}${barrnapDirArg}${genomeDirArg}${genomeQcDirArg}${genomeQcDirsArg}${idTokenIndexesArg}  --outdir "${asvMagLinkOutputDirAbs}" \\
  --threads ${asvMagLinkThreads} \\
  --min-pident ${asvMagLinkMinPident} \\
  --min-qcov ${asvMagLinkMinQcov} \\
  --top-n ${asvMagLinkTopN}

python "${plotAsvMagLinkScriptPath}" \\
  --input-dir "${asvMagLinkOutputDirAbs}" \\
  --top-n ${asvMagLinkPlotTopN}

touch asv_mag_link.done
"""
}

def downloadReference(String downloadUrl, File destination) {
    destination.parentFile?.mkdirs()
    def tmpFile = File.createTempFile("sina_ref", ".download", destination.parentFile ?: new File('.'))
    tmpFile.withOutputStream { out ->
        new java.net.URL(downloadUrl).withInputStream { ins ->
            out << ins
        }
    }
    if( downloadUrl?.toLowerCase()?.endsWith('.gz') ) {
        destination.withOutputStream { out ->
            tmpFile.withInputStream { tmpIn ->
                new java.util.zip.GZIPInputStream(tmpIn).withCloseable { gz ->
                    out << gz
                }
            }
        }
        tmpFile.delete()
    } else {
        if( !tmpFile.renameTo(destination) ) {
            tmpFile.withInputStream { ins ->
                destination.withOutputStream { out ->
                    out << ins
                }
            }
            tmpFile.delete()
        }
    }
}

def ensureBlastReferenceExists(String basePath, String fastaPath, String label){
    if( fastaPath ) {
        def fasta = new File(fastaPath)
        if( !fasta.exists() ) {
            exit 1, "${label} FASTA not found: ${fastaPath}"
        }
        return
    }
    def baseFile = new File(basePath)
    if( baseFile.exists() || ['.nhr','.nin','.nsq','.ndb'].any { new File(basePath + it).exists() } ) {
        return
    }
    exit 1, "${label} BLAST database not found: ${basePath}. Provide a database prefix or a FASTA input."
}

def writeNormalizedManifest(List records, File destination, String sourceManifest){
    destination.parentFile?.mkdirs()
    def seen = new LinkedHashSet<String>()
    def lines = ['sample_id\tfastq_r1\tfastq_r2']
    records.each { rec ->
        def sampleId = rec.sample_id?.toString()?.trim()
        if( !sampleId || !seen.add(sampleId) ) {
            exit 1, "Duplicate or empty sample ID in ${sourceManifest ?: 'FASTQ discovery'}: ${sampleId}"
        }
        lines << [sampleId, new File(rec.r1.toString()).canonicalPath,
                  rec.paired && rec.r2 ? new File(rec.r2.toString()).canonicalPath : ''].join('\t')
    }
    destination.text = lines.join(System.lineSeparator()) + System.lineSeparator()
    return destination.canonicalPath
}

def safeFilename(String value){
    return value.replaceAll(/[^A-Za-z0-9._-]+/, '_').replaceAll(/^_+|_+$/, '') ?: 'group'
}

def generatedPaletteColor(int index, int total){
    // Golden-angle hue spacing remains stable when metadata row order is unchanged.
    float hue = ((index * 0.61803398875d) % 1.0d) as float
    int rgb = java.awt.Color.HSBtoRGB(hue, 0.62f, 0.78f)
    return String.format('#%06X', rgb & 0xFFFFFF)
}

def prepareMetadataAssets(String metadataPath, String sampleCol, String groupCol, String colorCol,
                          String palettePath, File outputBase){
    File source = new File(metadataPath)
    def rows = source.readLines('UTF-8').findAll { it != null && !it.trim().isEmpty() }
    if( !rows ) {
        exit 1, "Metadata table is empty: ${metadataPath}"
    }
    def header = rows[0].split(/\t/, -1).collect { it.trim() }
    int sampleIdx = header.indexOf(sampleCol)
    int groupIdx = header.indexOf(groupCol)
    if( sampleIdx < 0 || groupIdx < 0 ) {
        exit 1, "Metadata must contain configured columns '${sampleCol}' and '${groupCol}': ${metadataPath}"
    }
    int colorIdx = header.indexOf(colorCol)
    def groups = []
    rows.drop(1).each { line ->
        def fields = line.split(/\t/, -1)
        if( fields.length > groupIdx ) {
            def value = fields[groupIdx].trim()
            if( value && !groups.contains(value) ) groups << value
        }
    }
    LinkedHashMap<String,String> palette = [:]
    if( palettePath ) {
        File paletteFile = new File(palettePath)
        if( !paletteFile.exists() ) exit 1, "Metadata palette file not found: ${palettePath}"
        def paletteRows = paletteFile.readLines('UTF-8').findAll { it?.trim() }
        paletteRows.eachWithIndex { line, idx ->
            def fields = line.split(/\t|,/, -1).collect { it.trim() }
            if( fields.size() >= 2 && !(idx == 0 && fields[0].equalsIgnoreCase('value')) ) {
                palette[fields[0]] = fields[1]
            }
        }
    }
    groups.eachWithIndex { group, idx ->
        if( !palette[group] && colorIdx >= 0 ) {
            def matching = rows.drop(1).find { line ->
                def fields = line.split(/\t/, -1)
                fields.length > Math.max(groupIdx, colorIdx) && fields[groupIdx].trim() == group && fields[colorIdx].trim()
            }
            if( matching ) palette[group] = matching.split(/\t/, -1)[colorIdx].trim()
        }
        if( !palette[group] ) palette[group] = generatedPaletteColor(idx, groups.size())
    }
    // Palette files may be shared across studies; publish only observed groups.
    LinkedHashMap<String,String> observedPalette = [:]
    groups.each { group -> observedPalette[group] = palette[group] }
    palette = observedPalette
    outputBase.parentFile?.mkdirs()
    File paletteOut = new File(outputBase.parentFile, "${outputBase.name}_palette.tsv")
    paletteOut.text = 'value\tcolor' + System.lineSeparator() + palette.collect { key, value -> "${key}\t${value}" }.join(System.lineSeparator()) + System.lineSeparator()
    File metadataOut = new File(outputBase.parentFile, "${outputBase.name}_metadata.tsv")
    def outputHeader = colorIdx >= 0 ? header : header + [colorCol]
    def outputRows = [outputHeader.join('\t')]
    rows.drop(1).each { line ->
        def fields = line.split(/\t/, -1).toList()
        def missingFields = header.size() - fields.size()
        if( missingFields > 0 ) {
            fields.addAll((1..missingFields).collect { '' })
        }
        def group = fields[groupIdx].trim()
        if( colorIdx >= 0 ) fields[colorIdx] = palette[group] ?: fields[colorIdx]
        else fields << (palette[group] ?: '')
        outputRows << fields.join('\t')
    }
    metadataOut.text = outputRows.join(System.lineSeparator()) + System.lineSeparator()
    return [metadata: metadataOut.canonicalPath, palette: paletteOut.canonicalPath]
}

def shellQuote(String value){
    if( value == null ){
        return "''"
    }
    return "'" + value.toString().replace("'", "'\"'\"'") + "'"
}

def joinShellArgs(List paths){
    if( !paths ) {
        return ''
    }
    return paths.collect { shellQuote(it.toString()) }.join(' ')
}

/**
 * Helpers
 */
def normalizeList(value, fallback){
    if( !value ) return fallback
    if( value instanceof List ) return value.collect { it.toString() }
    return value.toString().split(/\|/).collect { it.trim() }.findAll { it }
}

def normalizePresetList(value, fallback=[], presets=[:]){
    if( !value ) return fallback
    if( value instanceof List ) {
        return value.collect { it.toString().trim() }.findAll { it }
    }

    def text = value.toString().trim()
    if( !text ) return fallback

    def presetValue = presets[text]
    if( presetValue == null && text.endsWith('_order') ) {
        presetValue = presets[text.replaceFirst(/_order$/, '')]
    }
    if( presetValue != null && presetValue != value ) {
        return normalizePresetList(presetValue, fallback, presets)
    }

    return text.split(/[,|]/).collect { it.trim() }.findAll { it }
}

def compilePatterns(value, fallback){
    def list = value ?: fallback
    return list.collect { java.util.regex.Pattern.compile(it.toString()) }
}

def matchesExtension(String name, List<java.util.regex.Pattern> patterns){
    patterns.any { it.matcher(name).find() }
}

def isR1Like(String base, List<String> tokens){
    tokens.any { tok ->
        def rx = java.util.regex.Pattern.compile("(^|[_\\.\\-])${java.util.regex.Pattern.quote(tok)}([_\\.\\-]|\$)")
        rx.matcher(base).find()
    }
}

def sampleFromName(String baseName, String stripRegex, List<java.util.regex.Pattern> extPatterns){
    def base = baseName
    extPatterns.each { base = base.replaceAll(it, '') }
    base = base.replaceAll(stripRegex, '')
    base = base.replaceAll(/[_\-.]+$/, '')
    return base
}

def findR2File(File r1File, List<String> r1Tokens, List<String> r2Tokens){
    def original = r1File.name
    File matched = null
    (0..<r1Tokens.size()).each { i ->
        if( matched != null ) {
            return
        }
        def r1 = r1Tokens[i]
        def r2 = r2Tokens[i]
        def replacements = [
            ["_${java.util.regex.Pattern.quote(r1)}_", "_${r2}_"],
            ["\\.${java.util.regex.Pattern.quote(r1)}\\.", ".${r2}."],
            ["-${java.util.regex.Pattern.quote(r1)}-", "-${r2}-"],
            ["-${java.util.regex.Pattern.quote(r1)}\\.", "-${r2}."],
            ["_${java.util.regex.Pattern.quote(r1)}\\.", "_${r2}."],
            ["_${java.util.regex.Pattern.quote(r1)}\$", "_${r2}"],
            ["${java.util.regex.Pattern.quote(r1)}_001", "${r2}_001"]
        ]
        replacements.each { rep ->
            if( matched != null ) {
                return
            }
            def candidateName = original.replaceFirst(rep[0], rep[1])
            if( candidateName != original ){
                def candidate = new File(r1File.parentFile, candidateName)
                if( candidate.exists() ) {
                    matched = candidate
                }
            }
        }
    }
    return matched
}

def collectSampleRecords(String inputDirPath, List<String> r1Tokens, List<String> r2Tokens,
                         List<java.util.regex.Pattern> extPatterns, String stripRegex, boolean allowSingleEnd){
    File dir = new File(inputDirPath)
    if( !dir.exists() ){
        throw new IllegalArgumentException("Input directory does not exist: ${inputDirPath}")
    }
    def files = dir.listFiles()?.findAll { it.isFile() && matchesExtension(it.name, extPatterns) }?.sort { a, b -> a.name <=> b.name } ?: []
    def records = []
    files.each { file ->
        if( isR1Like(file.name, r1Tokens) ){
            def sampleId = sampleFromName(file.name, stripRegex, extPatterns)
            def r2File = findR2File(file, r1Tokens, r2Tokens)
            if( r2File ){
                records << [ sample_id: sampleId, paired: true, r1: file.canonicalPath, r2: r2File.canonicalPath ]
            } else if( allowSingleEnd ) {
                records << [ sample_id: sampleId, paired: false, r1: file.canonicalPath ]
            } else {
                log.warn "Skipping ${file.name}: no matching R2 detected."
            }
        }
    }
    return records
}

def loadManifestSamples(String manifestPath){
    File manifest = new File(manifestPath)
    if( !manifest.exists() ) {
        exit 1, "Manifest file not found: ${manifestPath}"
    }
    def records = []
    manifest.eachLine { line ->
        def trimmed = line.trim()
        if( !trimmed || trimmed.startsWith('#') ) {
            return
        }
        def parts = trimmed.split(/\t/)
        if( parts.length >= 2 && parts[0].trim().equalsIgnoreCase('sample_id') &&
            parts[1].trim().toLowerCase() in ['fastq_r1', 'r1', 'read1'] ) {
            return
        }
        if( parts.length < 2 ) {
            exit 1, "Manifest line must contain sample_id and R1 path separated by tab: ${line}"
        }
        def sampleId = parts[0].trim()
        def r1Path = resolveOptionalPath(parts[1].trim(), manifest.parentFile)
        def r2Path = parts.length > 2 && parts[2].trim() ? resolveOptionalPath(parts[2].trim(), manifest.parentFile) : null
        if( !sampleId ) {
            exit 1, "Sample ID missing in manifest line: ${line}"
        }
        if( !r1Path ) {
            exit 1, "R1 path missing in manifest line: ${line}"
        }
        def r1File = new File(r1Path)
        if( !r1File.exists() ) {
            exit 1, "R1 file not found for sample ${sampleId}: ${r1Path}"
        }
        File r2File = null
        boolean paired = false
        if( r2Path ) {
            r2File = new File(r2Path)
            if( !r2File.exists() ) {
                exit 1, "R2 file not found for sample ${sampleId}: ${r2Path}"
            }
            paired = true
        }
        records << [
            sample_id: sampleId,
            paired: paired,
            r1: r1File.canonicalPath,
            r2: paired ? r2File.canonicalPath : null
        ]
    }
    return records
}

def resolveOptionalPath(String pathValue, File baseDir){
    if( !pathValue ) return null
    def candidate = new File(pathValue)
    if( candidate.isAbsolute() ) {
        return candidate.canonicalPath
    }
    def baseCandidate = new File(baseDir ?: new File('.'), pathValue)
    if( baseCandidate.exists() ) {
        return baseCandidate.canonicalPath
    }
    def projectCandidate = new File(new File(projectDir.toString()), pathValue)
    if( projectCandidate.exists() ) {
        return projectCandidate.canonicalPath
    }
    return baseCandidate.canonicalPath
}

def resolveOutputRelative(String pathValue, String baseOutputDir){
    if( !pathValue ) return baseOutputDir
    def candidate = new File(pathValue)
    if( candidate.isAbsolute() ) {
        return candidate.canonicalPath
    }
    return new File(baseOutputDir ?: '.', pathValue).canonicalPath
}

def extractNamedStringMap(Map cfg, List<String> orderedKeys, String nestedKey, String legacySuffix){
    LinkedHashMap<String,String> out = [:]
    if( cfg?.get(nestedKey) instanceof Map ) {
        cfg[nestedKey].each { k, v ->
            def key = k?.toString()?.trim()
            def val = v?.toString()?.trim()
            if( key && val ) {
                out[key] = val
            }
        }
    }
    orderedKeys.eachWithIndex { key, idx ->
        def legacyKey = "group${idx + 1}_${legacySuffix}"
        if( cfg?.containsKey(legacyKey) ) {
            def val = cfg[legacyKey]?.toString()?.trim()
            if( val ) {
                out[key] = val
            }
        }
    }
    return out
}

def extractNamedListMap(Map cfg, List<String> orderedKeys, String nestedKey, String legacySuffix){
    LinkedHashMap<String,List<String>> out = [:]
    if( cfg?.get(nestedKey) instanceof Map ) {
        cfg[nestedKey].each { k, v ->
            def key = k?.toString()?.trim()
            def vals = []
            if( v instanceof List ) {
                vals = v.collect { it?.toString()?.trim() }.findAll { it }
            } else if( v ) {
                vals = v.toString().split(/[,|]/).collect { it.trim() }.findAll { it }
            }
            if( key && vals ) {
                out[key] = vals
            }
        }
    }
    orderedKeys.eachWithIndex { key, idx ->
        def legacyKey = "group${idx + 1}_${legacySuffix}"
        if( cfg?.containsKey(legacyKey) ) {
            def raw = cfg[legacyKey]
            def vals = []
            if( raw instanceof List ) {
                vals = raw.collect { it?.toString()?.trim() }.findAll { it }
            } else if( raw ) {
                vals = raw.toString().split(/[,|]/).collect { it.trim() }.findAll { it }
            }
            if( vals ) {
                out[key] = vals
            }
        }
    }
    return out
}
