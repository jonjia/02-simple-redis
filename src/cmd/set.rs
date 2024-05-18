use super::{extract_args, validate_command, CommandExecutor, SAdd, SIsMember, SMembers};
use crate::{cmd::CommandError, RespArray, RespFrame};

impl CommandExecutor for SAdd {
    fn execute(self, backend: &crate::Backend) -> RespFrame {
        let ret = backend.sadd(&self.key, &self.values);
        RespFrame::BulkString(format!("(integer) {}", ret).into())
    }
}

impl CommandExecutor for SMembers {
    fn execute(self, backend: &crate::Backend) -> RespFrame {
        let set = backend.smembers(&self.key);
        match set {
            Some(set) => {
                let mut data = Vec::with_capacity(set.len());
                for v in set.iter() {
                    let key = v.key().to_owned();
                    data.push(key);
                }
                if self.sort {
                    data.sort();
                }
                let ret = data
                    .into_iter()
                    .map(|k| RespFrame::BulkString(k.into()))
                    .collect::<Vec<RespFrame>>();
                RespArray::new(ret).into()
            }
            None => RespArray::new([]).into(),
        }
    }
}

impl CommandExecutor for SIsMember {
    fn execute(self, backend: &crate::Backend) -> RespFrame {
        let ret = backend.sismember(&self.key, self.value);
        RespFrame::BulkString(format!("(integer) {}", ret).into())
    }
}

impl TryFrom<RespArray> for SAdd {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        if value.len() < 3 {
            return Err(CommandError::InvalidArgument(
                "sadd command requires at least 2 parameters".to_string(),
            ));
        }

        let mut args = extract_args(value, 1)?.into_iter();
        match args.next() {
            Some(RespFrame::BulkString(key)) => {
                let values = args
                    .map(|f| match f {
                        RespFrame::BulkString(f) => Ok(String::from_utf8(f.0)?),
                        _ => Err(CommandError::InvalidArgument("Invalid field".to_string())),
                    })
                    .collect::<Result<Vec<String>, CommandError>>()?;
                let ret = SAdd {
                    key: String::from_utf8(key.0)?,
                    values,
                };
                Ok(ret)
            }
            _ => Err(CommandError::InvalidArgument("Invalid key".to_string())),
        }
    }
}

impl TryFrom<RespArray> for SMembers {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["smembers"], 1)?;

        let mut args = extract_args(value, 1)?.into_iter();
        match args.next() {
            Some(RespFrame::BulkString(key)) => Ok(SMembers {
                key: String::from_utf8(key.0)?,
                sort: false,
            }),
            _ => Err(CommandError::InvalidArgument("Invalid key".to_string())),
        }
    }
}

impl TryFrom<RespArray> for SIsMember {
    type Error = CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["sismember"], 2)?;

        let mut args = extract_args(value, 1)?.into_iter();
        match (args.next(), args.next()) {
            (Some(RespFrame::BulkString(key)), Some(RespFrame::BulkString(value))) => {
                Ok(SIsMember {
                    key: String::from_utf8(key.0)?,
                    value: String::from_utf8(value.0)?,
                })
            }
            _ => Err(CommandError::InvalidArgument(
                "Invalid key or value".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Backend, RespDecode};
    use anyhow::Result;
    use bytes::BytesMut;

    #[test]
    fn test_sadd_from_resp_array() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*3\r\n$4\r\nsadd\r\n$3\r\nset\r\n$5\r\nhello\r\n");
        let frame = RespArray::decode(&mut buf)?;

        let result: SAdd = frame.try_into()?;
        assert_eq!(result.key, "set");
        assert_eq!(result.values, vec!["hello".to_string()]);

        Ok(())
    }

    #[test]
    fn test_smembers_from_resp_array() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*2\r\n$8\r\nsmembers\r\n$3\r\nset\r\n");

        let frame = RespArray::decode(&mut buf)?;
        let result = SMembers::try_from(frame)?;
        assert_eq!(result.key, "set");
        assert!(!result.sort);
        Ok(())
    }

    #[test]
    fn test_sismember_from_resp_array() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*3\r\n$9\r\nsismember\r\n$3\r\nset\r\n$5\r\nhello\r\n");

        let frame = RespArray::decode(&mut buf)?;
        let result = SIsMember::try_from(frame)?;
        assert_eq!(result.key, "set");
        assert_eq!(result.value, "hello".to_string());

        Ok(())
    }

    #[test]
    fn test_sadd_sismember_smembers_command() -> Result<()> {
        let backend = Backend::new();
        let cmd = SAdd {
            key: "set".to_string(),
            values: vec!["hello".to_string(), "world".to_string()],
        };
        let result = cmd.execute(&backend);
        assert_eq!(result, RespFrame::BulkString("(integer) 2".into()));

        let cmd = SIsMember {
            key: "set".to_string(),
            value: "hello".to_string(),
        };
        let result = cmd.execute(&backend);
        assert_eq!(result, RespFrame::BulkString("(integer) 1".into()));

        let cmd = SMembers {
            key: "set".to_string(),
            sort: true,
        };

        let result = cmd.execute(&backend);
        assert_eq!(
            result,
            RespArray::new(vec![
                RespFrame::BulkString("hello".into()),
                RespFrame::BulkString("world".into())
            ])
            .into()
        );

        let cmd = SIsMember {
            key: "set".to_string(),
            value: "not_member".to_string(),
        };
        let result = cmd.execute(&backend);
        assert_eq!(result, RespFrame::BulkString("(integer) 0".into()));

        let cmd = SIsMember {
            key: "key_not_exist".to_string(),
            value: "whatever".to_string(),
        };
        let result = cmd.execute(&backend);
        assert_eq!(result, RespFrame::BulkString("(integer) 0".into()));

        Ok(())
    }
}
