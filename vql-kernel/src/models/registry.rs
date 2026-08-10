use std::collections::{BTreeMap, HashMap};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde_json::Value;

use super::ort_backend::OrtRuntimeFactory;
use super::pipeline::{
    CompiledPipeline, PostProcessor, PreProcessor, RuntimeSession, TensorContract,
};
use super::postprocess::YoloPostProcessorFactory;
use super::preprocess::ImageTensorFactory;
use super::triton_backend::TritonRuntimeFactory;
use crate::catalog::{ModelDef, ModelType, ProcessorSpec, RuntimeSpec};
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

pub(super) trait RuntimeFactory: Send + Sync + Debug {
    fn kind(&self) -> &str;
    fn supported_types(&self) -> &[ModelType];
    fn validate(&self, source: &str, spec: &RuntimeSpec) -> Result<()>;
    fn build(
        &self,
        model: &ModelDef,
        input: &TensorContract,
        output: &TensorContract,
    ) -> Result<Arc<dyn RuntimeSession>>;
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
        registry.register_post_processor(Arc::new(YoloPostProcessorFactory::end_to_end()));
        registry.register_post_processor(Arc::new(YoloPostProcessorFactory::raw()));
        registry.register_post_processor(Arc::new(YoloPostProcessorFactory::xywh_normalized()));
        registry.register_runtime(Arc::new(OrtRuntimeFactory));
        registry.register_runtime(Arc::new(TritonRuntimeFactory));
        registry.register_runtime(Arc::new(UnavailableRuntimeFactory::new(
            "transformers",
            "v0.4",
        )));
        for kind in ["vllm", "sglang", "llama_cpp"] {
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

    pub(crate) fn model_specs_for_options(
        &self,
        model_type: ModelType,
        source: &str,
        options: &BTreeMap<String, Value>,
    ) -> Result<(RuntimeSpec, ProcessorSpec, ProcessorSpec)> {
        let default_runtime = if source.starts_with("endpoint://") {
            "triton"
        } else {
            "onnxruntime"
        };
        let mut runtime = RuntimeSpec {
            kind: default_runtime.to_owned(),
            protocol: None,
            options: BTreeMap::new(),
        };
        let mut pre_processor = ProcessorSpec {
            kind: "vision.image_tensor@1".to_owned(),
            options: BTreeMap::new(),
        };
        let mut post_processor = ProcessorSpec {
            kind: "vision.yolo_e2e@1".to_owned(),
            options: BTreeMap::new(),
        };

        for (name, value) in options {
            match name.as_str() {
                "runtime.kind" => runtime.kind = string_value(name, value)?.to_ascii_lowercase(),
                "runtime.protocol" => {
                    runtime.protocol = Some(string_value(name, value)?.to_ascii_lowercase())
                }
                "pre_processor.kind" => {
                    pre_processor.kind = string_value(name, value)?.to_ascii_lowercase()
                }
                "pre_processor.options" => {
                    pre_processor.options = object_value(name, value)?;
                }
                "post_processor.kind" => {
                    post_processor.kind = string_value(name, value)?.to_ascii_lowercase()
                }
                "post_processor.options" => {
                    post_processor.options = object_value(name, value)?;
                }
                _ if name.starts_with("runtime.") => {
                    let option = name.trim_start_matches("runtime.");
                    if !matches!(option, "model_name" | "model_version") {
                        return invalid_option(name, "unknown Runtime option");
                    }
                    runtime.options.insert(option.to_owned(), value.clone());
                }
                _ => return invalid_option(name, "unknown Model option"),
            }
        }
        if runtime.kind == "triton" && runtime.protocol.is_none() {
            runtime.protocol = Some("kserve_v2_http".to_owned());
        }
        self.validate_specs(
            model_type,
            source,
            &runtime,
            &pre_processor,
            &post_processor,
        )?;
        Ok((runtime, pre_processor, post_processor))
    }

    pub(crate) fn validate_model(&self, model: &ModelDef) -> Result<()> {
        self.validate_specs(
            model.model_type,
            &model.source,
            &model.runtime,
            &model.pre_processor,
            &model.post_processor,
        )
    }

    pub(super) fn compile(&self, model: &ModelDef) -> Result<CompiledPipeline> {
        self.validate_model(model)?;
        let pre_factory = self.pre_processor(model.model_type, &model.pre_processor)?;
        let post_factory = self.post_processor(model.model_type, &model.post_processor)?;
        let runtime_factory = self.runtime(model.model_type, &model.runtime)?;
        let pre_processor = pre_factory.build(&model.pre_processor)?;
        let post_processor = post_factory.build(&model.post_processor)?;
        let runtime = runtime_factory.build(
            model,
            pre_processor.runtime_output(),
            post_processor.runtime_input(),
        )?;
        CompiledPipeline::try_new(pre_processor, runtime, post_processor)
    }

    fn validate_specs(
        &self,
        model_type: ModelType,
        source: &str,
        runtime: &RuntimeSpec,
        pre_processor: &ProcessorSpec,
        post_processor: &ProcessorSpec,
    ) -> Result<()> {
        let runtime_factory = self.runtime(model_type, runtime)?;
        runtime_factory.validate(source, runtime)?;
        let pre_factory = self.pre_processor(model_type, pre_processor)?;
        pre_factory.validate(pre_processor)?;
        let post_factory = self.post_processor(model_type, post_processor)?;
        post_factory.validate(post_processor)
    }

    fn pre_processor(
        &self,
        model_type: ModelType,
        spec: &ProcessorSpec,
    ) -> Result<&dyn PreProcessorFactory> {
        let Some(factory) = self.pre_processors.get(&spec.kind) else {
            return invalid_option(
                "pre_processor.kind",
                format!(
                    "unsupported {} PreProcessor '{}'",
                    model_type_name(model_type),
                    spec.kind
                ),
            );
        };
        ensure_supported(
            "pre_processor.kind",
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
                "post_processor.kind",
                format!(
                    "unsupported {} PostProcessor '{}'",
                    model_type_name(model_type),
                    spec.kind
                ),
            );
        };
        ensure_supported(
            "post_processor.kind",
            "PostProcessor",
            model_type,
            &spec.kind,
            factory.supported_types(),
        )?;
        Ok(factory.as_ref())
    }

    fn runtime(&self, model_type: ModelType, spec: &RuntimeSpec) -> Result<&dyn RuntimeFactory> {
        let Some(factory) = self.runtimes.get(&spec.kind) else {
            return invalid_option(
                "runtime.kind",
                format!("unsupported v0.1 Runtime '{}'", spec.kind),
            );
        };
        ensure_supported(
            "runtime.kind",
            "Runtime",
            model_type,
            &spec.kind,
            factory.supported_types(),
        )?;
        Ok(factory.as_ref())
    }
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
    match model_type {
        ModelType::ObjectDetection => "OBJECT_DETECTION",
    }
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

impl RuntimeFactory for UnavailableRuntimeFactory {
    fn kind(&self) -> &str {
        self.kind
    }

    fn supported_types(&self) -> &[ModelType] {
        &[ModelType::ObjectDetection]
    }

    fn validate(&self, _source: &str, _spec: &RuntimeSpec) -> Result<()> {
        self.unavailable()
    }

    fn build(
        &self,
        _model: &ModelDef,
        _input: &TensorContract,
        _output: &TensorContract,
    ) -> Result<Arc<dyn RuntimeSession>> {
        self.unavailable()
    }
}

pub(super) fn deserialize_processor_options<T: DeserializeOwned>(
    namespace: &str,
    options: &BTreeMap<String, Value>,
) -> Result<T> {
    deserialize_options(namespace, options, "unknown processor option")
}

pub(super) fn deserialize_runtime_options<T: DeserializeOwned>(
    options: &BTreeMap<String, Value>,
) -> Result<T> {
    deserialize_options("runtime", options, "unknown Runtime option")
}

fn deserialize_options<T: DeserializeOwned>(
    namespace: &str,
    options: &BTreeMap<String, Value>,
    unknown_message: &str,
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
            invalid_option_error(format!("{namespace}.{field}"), unknown_message)
        } else {
            invalid_option_error(namespace, message)
        }
    })
}

fn object_value(name: &str, value: &Value) -> Result<BTreeMap<String, Value>> {
    let object = value
        .as_object()
        .ok_or_else(|| invalid_option_error(name, "must be an object"))?;
    Ok(object
        .iter()
        .map(|(key, value)| (key.to_ascii_lowercase(), value.clone()))
        .collect())
}

fn string_value(name: &str, value: &Value) -> Result<String> {
    value
        .as_str()
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| invalid_option_error(name, "must be a non-empty string"))
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
    use crate::models::backend::ModelBackend;
    use image::DynamicImage;
    use tokio_util::sync::CancellationToken;

    #[derive(Debug)]
    struct WrongTypePreProcessorFactory;

    impl PreProcessorFactory for WrongTypePreProcessorFactory {
        fn kind(&self) -> &str {
            "test.wrong_type@1"
        }

        fn supported_types(&self) -> &[ModelType] {
            &[]
        }

        fn validate(&self, _spec: &ProcessorSpec) -> Result<()> {
            Ok(())
        }

        fn build(&self, _spec: &ProcessorSpec) -> Result<Arc<dyn PreProcessor>> {
            unreachable!("type compatibility is checked before factory build")
        }
    }

    #[test]
    fn registry_rejects_unknown_kind_with_stable_error() {
        let registry = PipelineRegistry::builtins();
        let error = registry
            .model_specs_for_options(
                ModelType::ObjectDetection,
                "mock://person",
                &BTreeMap::from([(
                    "post_processor.kind".to_owned(),
                    Value::String("unknown".to_owned()),
                )]),
            )
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::InvalidOption);
        assert_eq!(
            error.message,
            "invalid Model option 'post_processor.kind': unsupported OBJECT_DETECTION PostProcessor 'unknown'"
        );
    }

    #[test]
    fn typed_options_report_the_complete_unknown_field_path() {
        let registry = PipelineRegistry::builtins();
        let error = registry
            .model_specs_for_options(
                ModelType::ObjectDetection,
                "mock://person",
                &BTreeMap::from([(
                    "pre_processor.options".to_owned(),
                    serde_json::json!({"widht": 640}),
                )]),
            )
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::InvalidOption);
        assert!(error.message.contains("pre_processor.options.widht"));
        assert!(error.message.contains("unknown processor option"));
    }

    #[test]
    fn registry_rejects_a_kind_and_model_type_mismatch() {
        let mut registry = PipelineRegistry::builtins();
        registry.register_pre_processor(Arc::new(WrongTypePreProcessorFactory));
        let error = registry
            .model_specs_for_options(
                ModelType::ObjectDetection,
                "mock://person",
                &BTreeMap::from([(
                    "pre_processor.kind".to_owned(),
                    Value::String("test.wrong_type@1".to_owned()),
                )]),
            )
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::InvalidOption);
        assert!(error.message.contains("OBJECT_DETECTION PreProcessor"));
        assert!(error.message.contains("test.wrong_type@1"));
    }

    #[test]
    fn known_future_runtime_is_version_gated_by_a_stub_factory() {
        let registry = PipelineRegistry::builtins();
        let error = registry
            .model_specs_for_options(
                ModelType::ObjectDetection,
                "file:///model.onnx",
                &BTreeMap::from([(
                    "runtime.kind".to_owned(),
                    Value::String("transformers".to_owned()),
                )]),
            )
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::FeatureNotAvailable);
        assert_eq!(error.target_version.as_deref(), Some("v0.4"));
    }

    #[tokio::test]
    #[ignore = "requires VQL_YOLO26_ONNX"]
    async fn real_yolo26_onnx_e2e() {
        let path = std::env::var_os("VQL_YOLO26_ONNX")
            .map(std::path::PathBuf::from)
            .expect("set VQL_YOLO26_ONNX to an exported YOLO26 ONNX model");
        let registry = PipelineRegistry::builtins();
        let source = format!("file://{}", path.display());
        let (runtime, pre_processor, post_processor) = registry
            .model_specs_for_options(ModelType::ObjectDetection, &source, &BTreeMap::new())
            .unwrap();
        let model = ModelDef {
            name: "detector".to_owned(),
            model_type: ModelType::ObjectDetection,
            source,
            resolved_source: path.to_string_lossy().into_owned(),
            artifact_hash: None,
            runtime,
            pre_processor,
            post_processor,
            semantic_fingerprint: "real-yolo26".to_owned(),
            volatile: false,
        };
        let pipeline = registry.compile(&model).expect("compile YOLO pipeline");
        // More than one image per call: a batch of 1 hides execution providers that cannot
        // honour the dynamic batch dimension.
        let output = pipeline
            .infer(
                vec![
                    DynamicImage::new_rgb8(640, 480),
                    DynamicImage::new_rgb8(1280, 720),
                    DynamicImage::new_rgb8(320, 320),
                ],
                CancellationToken::new(),
            )
            .await
            .expect("run YOLO ONNX inference");
        assert_eq!(output.len(), 3);
    }
}
