use serde::{self, Deserialize, Serialize};

pub type KeyValue = u64;
pub type ElementValue = u64;

pub type Message = super::Message<BodyData>;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum BodyData {
    Init(super::InitData),
    InitOk,
    Txn(TxnData),
    TxnOk(TxnData),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TxnData {
    pub txn: Vec<TxnFunc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(into = "TxnFuncRepr")]
#[serde(try_from = "TxnFuncRepr")]
pub enum TxnFunc {
    Read {
        key: KeyValue,
        value: Option<Vec<ElementValue>>,
    },
    Append {
        key: KeyValue,
        element: ElementValue,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum ValueRepr {
    R(Option<Vec<ElementValue>>),
    Append(ElementValue),
}

const READ_FUNC_REPR: &str = "r";
const APPEND_FUNC_REPR: &str = "append";

#[derive(Debug, Serialize, Deserialize)]
struct TxnFuncRepr(String, KeyValue, ValueRepr);

impl TryFrom<TxnFuncRepr> for TxnFunc {
    type Error = std::io::Error;

    fn try_from(repr: TxnFuncRepr) -> Result<Self, Self::Error> {
        let TxnFuncRepr(f, k, v) = repr;
        match (f.as_str(), v) {
            (READ_FUNC_REPR, ValueRepr::R(value)) => Ok(Self::Read { key: k, value }),
            (APPEND_FUNC_REPR, ValueRepr::Append(element)) => Ok(Self::Append { key: k, element }),
            (f, v) => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Invalid repr: [{f}, {k}, {v:?}]'"),
            )),
        }
    }
}

impl Into<TxnFuncRepr> for TxnFunc {
    fn into(self) -> TxnFuncRepr {
        match self {
            TxnFunc::Read { key, value } => {
                TxnFuncRepr(READ_FUNC_REPR.to_string(), key, ValueRepr::R(value))
            }
            TxnFunc::Append { key, element } => TxnFuncRepr(
                APPEND_FUNC_REPR.to_string(),
                key,
                ValueRepr::Append(element),
            ),
        }
    }
}

#[cfg(test)]
mod txn_func_serde_tests {
    use super::TxnFunc;
    use serde_json::json;

    #[test]
    fn empty_read() {
        check(
            TxnFunc::Read {
                key: 4,
                value: None,
            },
            json!(["r", 4, null]),
        );
    }

    #[test]
    fn value_read() {
        check(
            TxnFunc::Read {
                key: 4,
                value: Some(vec![1, 2]),
            },
            json!(["r", 4, [1, 2]]),
        );
    }

    #[test]
    fn append() {
        check(
            TxnFunc::Append { key: 2, element: 1 },
            json!(["append", 2, 1]),
        );
    }

    fn check(f: TxnFunc, expected: serde_json::Value) {
        assert_eq!(serde_json::to_value(f.clone()).unwrap(), expected);
        assert_eq!(serde_json::from_value::<TxnFunc>(expected).unwrap(), f);
    }
}
