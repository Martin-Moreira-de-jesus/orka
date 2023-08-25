use prost_types::Any;

pub fn default_spec() -> Any {
    let spec = include_str!("manifests/oci-template.json").to_string();

    let spec = Any {
        type_url: "types.containerd.io/opencontainers/runtime-spec/1/Spec".to_string(),
        value: spec.into_bytes(),
    };

    spec
}
