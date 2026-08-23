use std::collections::{BTreeMap, HashMap};
use std::fmt::{Debug, Formatter};
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use super::backend::ModelBackend;
use super::definition::semantic_fingerprint;
use super::ort_backend::OrtRuntimeFactory;
use super::pipeline::{
    BatchingOwner, CompiledPipeline, PostProcessor, PreProcessor, RuntimeSession, TensorContract,
};
use super::postprocess::{ClassificationPostProcessorFactory, YoloPostProcessorFactory};
use super::preprocess::ImageTensorFactory;
use super::triton_backend::TritonRuntimeFactory;
use crate::catalog::{
    ModelInterface, ModelType, ModelVersion, ProcessorSpec, ResolvedExecutionSpec,
    ResolvedModelDef, ResolvedModelSpec, RuntimeSpec,
};
use crate::{ErrorCode, Result, VqlError};

pub(super) trait PreProcessorFactory: Send + Sync + Debug {
    fn kind(&self) -> &str;
    fn supported_types(&self) -> &[ModelType];
    fn validate(&self, spec: &ProcessorSpec) -> Result<()>;
    fn build(&self, spec: &ProcessorSpec) -> Result<Arc<dyn PreProcessor>>;
}

pub(super) trait PostProcessorFactory: Send + Sync + Debug {
    fn kind(&self) -> &str;
    fn supported_types(&self) -> &[ModelType];
    fn validate(&self, spec: &ProcessorSpec) -> Result<()>;
    fn build(&self, spec: &ProcessorSpec) -> Result<Arc<dyn PostProcessor>>;
}

pub(super) struct RuntimeResolution {
    pub(super) resolved_source: String,
    pub(super) artifact_hash: Option<String>,
    pub(super) execution: ResolvedExecutionSpec,
    pub(super) volatile: bool,
}

#[async_trait]
pub(super) trait RuntimeFactory: Send + Sync + Debug {
    fn kind(&self) -> &str;
    fn supported_types(&self) -> &[ModelType];
    fn validate_declaration(
        &self,
        interface: &ModelInterface,
        version: &ModelVersion,
    ) -> Result<()>;
    async fn resolve(
        &self,
        interface: &ModelInterface,
        version: &ModelVersion,
        cache_dir: &Path,
        cancel: CancellationToken,
    ) -> Result<RuntimeResolution>;

    fn build_embedded(
        &self,
        _model: &ResolvedModelDef,
        _runtime: &RuntimeSpec,
        _input: &TensorContract,
        _output: &TensorContract,
    ) -> Result<Arc<dyn RuntimeSession>> {
        Err(VqlError::new(
            ErrorCode::Internal,
            format!("Runtime '{}' is not an embedded Runtime", self.kind()),
        ))
    }

    fn build_service(
        &self,
        _model: &ResolvedModelDef,
        _runtime: &RuntimeSpec,
    ) -> Result<Arc<dyn ModelBackend>> {
        Err(VqlError::new(
            ErrorCode::Internal,
            format!("Runtime '{}' is not a service Runtime", self.kind()),
        ))
    }
}

pub(crate) struct PipelineRegistry {
    pre_processors: HashMap<String, Arc<dyn PreProcessorFactory>>,
    runtimes: HashMap<String, Arc<dyn RuntimeFactory>>,
    post_processors: HashMap<String, Arc<dyn PostProcessorFactory>>,
}

impl Debug for PipelineRegistry {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PipelineRegistry")
            .field("pre_processors", &self.pre_processors.keys())
            .field("runtimes", &self.runtimes.keys())
            .field("post_processors", &self.post_processors.keys())
            .finish()
    }
}

impl PipelineRegistry {
    pub(crate) fn builtins() -> Self {
        let mut registry = Self {
            pre_processors: HashMap::new(),
            runtimes: HashMap::new(),
            post_processors: HashMap::new(),
        };
        registry.register_pre_processor(Arc::new(ImageTensorFactory));
        registry.register_post_processor(Arc::new(ClassificationPostProcessorFactory));
        registry.register_post_processor(Arc::new(YoloPostProcessorFactory::end_to_end()));
        registry.register_post_processor(Arc::new(YoloPostProcessorFactory::raw()));
        registry.register_post_processor(Arc::new(YoloPostProcessorFactory::xywh_normalized()));
        registry.register_runtime(Arc::new(OrtRuntimeFactory));
        registry.register_runtime(Arc::new(TritonRuntimeFactory));
        registry.register_runtime(Arc::new(UnavailableRuntimeFactory::new(
            "transformers",
            "v0.3",
        )));
        for kind in ["vllm", "sglang", "llama-cpp"] {
            registry.register_runtime(Arc::new(UnavailableRuntimeFactory::new(kind, "未排期")));
        }
        registry
    }

    fn register_pre_processor(&mut self, factory: Arc<dyn PreProcessorFactory>) {
        self.pre_processors
            .insert(factory.kind().to_owned(), factory);
    }

    fn register_post_processor(&mut self, factory: Arc<dyn PostProcessorFactory>) {
        self.post_processors
            .insert(factory.kind().to_owned(), factory);
    }

    fn register_runtime(&mut self, factory: Arc<dyn RuntimeFactory>) {
        self.runtimes.insert(factory.kind().to_owned(), factory);
    }

    pub(crate) fn validate_declaration(
        &self,
        interface: &ModelInterface,
        version: &ModelVersion,
    ) -> Result<()> {
        let factory = self.runtime_for_interface(interface, &version.runtime_kind)?;
        factory.validate_declaration(interface, version)
    }

    pub(crate) async fn resolve_version(
        &self,
        interface: &ModelInterface,
        version: &ModelVersion,
        cache_dir: &Path,
        cancel: CancellationToken,
    ) -> Result<ResolvedModelSpec> {
        self.validate_declaration(interface, version)?;
        let factory = self.runtime_for_interface(interface, &version.runtime_kind)?;
        let resolution = factory
            .resolve(interface, version, cache_dir, cancel)
            .await?;
        self.validate_execution(interface, &version.runtime_kind, &resolution.execution)?;
        let fingerprint = semantic_fingerprint(&ResolvedFingerprint {
            declaration_fingerprint: &version.declaration_fingerprint,
            resolved_source: &resolution.resolved_source,
            artifact_hash: resolution.artifact_hash.as_deref(),
            execution: &resolution.execution,
            volatile: resolution.volatile,
        });
        Ok(ResolvedModelSpec {
            resolved_source: resolution.resolved_source,
            artifact_hash: resolution.artifact_hash,
            execution: resolution.execution,
            semantic_fingerprint: fingerprint,
            volatile: resolution.volatile,
        })
    }

    pub(super) fn compile_backend(
        &self,
        model: &ResolvedModelDef,
    ) -> Result<(Arc<dyn ModelBackend>, BatchingOwner)> {
        match &model.execution {
            ResolvedExecutionSpec::Embedded {
                runtime,
                pre_processor,
                post_processor,
            } => {
                let model_type = execution_model_type(&model.interface);
                self.validate_execution(&model.interface, &runtime.kind, &model.execution)?;
                let pre_factory = self.pre_processor(model_type, pre_processor)?;
                let post_factory = self.post_processor(model_type, post_processor)?;
                let runtime_factory = self.runtime(model_type, &runtime.kind)?;
                let pre_processor = pre_factory.build(pre_processor)?;
                let post_processor = post_factory.build(post_processor)?;
                let runtime = runtime_factory.build_embedded(
                    model,
                    runtime,
                    pre_processor.runtime_output(),
                    post_processor.runtime_input(),
                )?;
                let pipeline = CompiledPipeline::try_new(pre_processor, runtime, post_processor)?;
                let batching_owner = pipeline.batching_owner();
                Ok((Arc::new(pipeline), batching_owner))
            }
            ResolvedExecutionSpec::Service { runtime } => {
                let model_type = execution_model_type(&model.interface);
                self.validate_execution(&model.interface, &runtime.kind, &model.execution)?;
                let runtime_factory = self.runtime(model_type, &runtime.kind)?;
                Ok((
                    runtime_factory.build_service(model, runtime)?,
                    BatchingOwner::Service,
                ))
            }
            ResolvedExecutionSpec::Generic { .. } => Err(VqlError::new(
                ErrorCode::Internal,
                "generic Model execution does not use the vision capability pipeline",
            )),
        }
    }

    fn validate_execution(
        &self,
        interface: &ModelInterface,
        declared_runtime: &str,
        execution: &ResolvedExecutionSpec,
    ) -> Result<()> {
        let runtime = match execution {
            ResolvedExecutionSpec::Embedded { runtime, .. }
            | ResolvedExecutionSpec::Service { runtime }
            | ResolvedExecutionSpec::Generic { runtime, .. } => runtime,
        };
        if runtime.kind != declared_runtime {
            return Err(VqlError::new(
                ErrorCode::Internal,
                "resolved Runtime does not match the Model declaration",
            ));
        }
        self.runtime_for_interface(interface, &runtime.kind)?;
        if let ResolvedExecutionSpec::Embedded {
            pre_processor,
            post_processor,
            ..
        } = execution
        {
            let model_type = execution_model_type(interface);
            self.pre_processor(model_type, pre_processor)?
                .validate(pre_processor)?;
            self.post_processor(model_type, post_processor)?
                .validate(post_processor)?;
        }
        Ok(())
    }

    fn pre_processor(
        &self,
        model_type: ModelType,
        spec: &ProcessorSpec,
    ) -> Result<&dyn PreProcessorFactory> {
        let Some(factory) = self.pre_processors.get(&spec.kind) else {
            return invalid_option(
                "input.format",
                format!(
                    "unsupported {} PreProcessor '{}'",
                    model_type_name(model_type),
                    spec.kind
                ),
            );
        };
        ensure_supported(
            "input.format",
            "PreProcessor",
            model_type,
            &spec.kind,
            factory.supported_types(),
        )?;
        Ok(factory.as_ref())
    }

    fn post_processor(
        &self,
        model_type: ModelType,
        spec: &ProcessorSpec,
    ) -> Result<&dyn PostProcessorFactory> {
        let Some(factory) = self.post_processors.get(&spec.kind) else {
            return invalid_option(
                "output.format",
                format!(
                    "unsupported {} PostProcessor '{}'",
                    model_type_name(model_type),
                    spec.kind
                ),
            );
        };
        ensure_supported(
            "output.format",
            "PostProcessor",
            model_type,
            &spec.kind,
            factory.supported_types(),
        )?;
        Ok(factory.as_ref())
    }

    fn runtime(&self, model_type: ModelType, kind: &str) -> Result<&dyn RuntimeFactory> {
        let Some(factory) = self.runtimes.get(kind) else {
            return invalid_option("USING", format!("unsupported Runtime '{kind}'"));
        };
        ensure_supported(
            "USING",
            "Runtime",
            model_type,
            kind,
            factory.supported_types(),
        )?;
        Ok(factory.as_ref())
    }

    fn runtime_for_interface(
        &self,
        interface: &ModelInterface,
        kind: &str,
    ) -> Result<&dyn RuntimeFactory> {
        if let Some(model_type) = interface.capability {
            return self.runtime(model_type, kind);
        }
        if kind != "onnx-runtime" {
            return Err(VqlError::new(
                ErrorCode::InvalidOption,
                format!(
                    "USING: generic Model signatures require the embedded ONNX_RUNTIME, found '{kind}'"
                ),
            ));
        }
        self.runtimes.get(kind).map(AsRef::as_ref).ok_or_else(|| {
            VqlError::new(
                ErrorCode::InvalidOption,
                format!("OPTIONS: unsupported Runtime '{kind}'"),
            )
        })
    }
}

#[derive(Serialize)]
struct ResolvedFingerprint<'a> {
    declaration_fingerprint: &'a str,
    resolved_source: &'a str,
    artifact_hash: Option<&'a str>,
    execution: &'a ResolvedExecutionSpec,
    volatile: bool,
}

fn ensure_supported(
    option: &str,
    stage: &str,
    model_type: ModelType,
    kind: &str,
    supported: &[ModelType],
) -> Result<()> {
    if supported.contains(&model_type) {
        Ok(())
    } else {
        invalid_option(
            option,
            format!(
                "unsupported {} {stage} '{kind}'",
                model_type_name(model_type)
            ),
        )
    }
}

const fn model_type_name(model_type: ModelType) -> &'static str {
    model_type.as_str()
}

fn execution_model_type(interface: &ModelInterface) -> ModelType {
    interface.capability.unwrap_or(ModelType::ObjectDetection)
}

#[derive(Debug)]
struct UnavailableRuntimeFactory {
    kind: &'static str,
    target_version: &'static str,
}

impl UnavailableRuntimeFactory {
    const fn new(kind: &'static str, target_version: &'static str) -> Self {
        Self {
            kind,
            target_version,
        }
    }

    fn unavailable<T>(&self) -> Result<T> {
        Err(VqlError::feature(
            format!("the {} Runtime is not available", self.kind),
            self.target_version,
        ))
    }
}

#[async_trait]
impl RuntimeFactory for UnavailableRuntimeFactory {
    fn kind(&self) -> &str {
        self.kind
    }

    fn supported_types(&self) -> &[ModelType] {
        &[ModelType::ObjectDetection]
    }

    fn validate_declaration(
        &self,
        _interface: &ModelInterface,
        _version: &ModelVersion,
    ) -> Result<()> {
        self.unavailable()
    }

    async fn resolve(
        &self,
        _interface: &ModelInterface,
        _version: &ModelVersion,
        _cache_dir: &Path,
        _cancel: CancellationToken,
    ) -> Result<RuntimeResolution> {
        self.unavailable()
    }
}

pub(super) fn deserialize_processor_options<T: DeserializeOwned>(
    namespace: &str,
    options: &BTreeMap<String, Value>,
) -> Result<T> {
    deserialize_options(namespace, options, "unknown processor option")
}

pub(super) fn deserialize_model_options<T: DeserializeOwned>(
    runtime: &str,
    options: &BTreeMap<String, Value>,
) -> Result<T> {
    deserialize_options(
        "OPTIONS",
        options,
        format!("unknown {runtime} Model option"),
    )
}

fn deserialize_options<T: DeserializeOwned>(
    namespace: &str,
    options: &BTreeMap<String, Value>,
    unknown_message: impl AsRef<str>,
) -> Result<T> {
    let value = serde_json::to_value(options).map_err(|error| {
        VqlError::new(ErrorCode::Internal, "failed to serialize Model options").with_source(error)
    })?;
    serde_json::from_value(value).map_err(|error| {
        let message = error.to_string();
        if let Some(field) = message
            .strip_prefix("unknown field `")
            .and_then(|value| value.split('`').next())
        {
            invalid_option_error(format!("{namespace}.{field}"), unknown_message.as_ref())
        } else {
            invalid_option_error(namespace, message)
        }
    })
}

pub(super) fn invalid_option<T>(name: impl AsRef<str>, message: impl Into<String>) -> Result<T> {
    Err(invalid_option_error(name, message))
}

fn invalid_option_error(name: impl AsRef<str>, message: impl Into<String>) -> VqlError {
    VqlError::new(
        ErrorCode::InvalidOption,
        format!(
            "invalid Model option '{}': {}",
            name.as_ref(),
            message.into()
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn declaration(runtime_kind: &str) -> (ModelInterface, ModelVersion) {
        let interface = ModelInterface {
            capability: Some(ModelType::ObjectDetection),
            parameters: Vec::new(),
            semantic_arguments: Vec::new(),
            return_type: "detections".to_owned(),
            processing_family: "vision.object_detection".to_owned(),
            deterministic: true,
        };
        let version = ModelVersion {
            name: "v1".to_owned(),
            source: "mock://person".to_owned(),
            runtime_kind: runtime_kind.to_owned(),
            options: BTreeMap::new(),
            declaration_fingerprint: "declaration".to_owned(),
            resolved: None,
            created_at: 0,
        };
        (interface, version)
    }

    #[test]
    fn registry_rejects_unknown_runtime_with_stable_error() {
        let registry = PipelineRegistry::builtins();
        let (interface, version) = declaration("unknown");
        let error = registry
            .validate_declaration(&interface, &version)
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::InvalidOption);
        assert!(error.message.contains("USING"));
        assert!(error.message.contains("unknown"));
    }

    #[test]
    fn known_future_runtime_is_version_gated_by_a_stub_factory() {
        let registry = PipelineRegistry::builtins();
        let (interface, version) = declaration("transformers");
        let error = registry
            .validate_declaration(&interface, &version)
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::FeatureNotAvailable);
        assert_eq!(error.target_version.as_deref(), Some("v0.3"));
    }
}
