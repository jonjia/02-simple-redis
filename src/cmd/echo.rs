use crate::{RespArray, RespFrame};

use super::{extract_args, validate_command, CommandError, CommandExecutor, Echo};

impl CommandExecutor for Echo {
    fn execute(self, _backend: &crate::Backend) -> RespFrame {
        RespFrame::BulkString(self.value.into())
    }
}

impl TryFrom<RespArray> for Echo {
    type Error = crate::cmd::CommandError;
    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["echo"], 1)?;

        let mut args = extract_args(value, 1)?.into_iter();
        match args.next() {
            Some(RespFrame::BulkString(key)) => Ok(Echo {
                value: String::from_utf8(key.0)?,
            }),
            _ => Err(CommandError::InvalidArgument("Invalid key".to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Backend, RespArray, RespDecode};
    use anyhow::Result;
    use bytes::BytesMut;

    #[test]
    fn test_echo_from_resp_array() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*2\r\n$4\r\necho\r\n$5\r\nhello\r\n");

        let frame = RespArray::decode(&mut buf)?;

        let result: Echo = frame.try_into()?;
        assert_eq!(result.value, "hello");

        Ok(())
    }

    #[test]
    fn test_echo_command() -> Result<()> {
        let backend = Backend::new();
        let cmd = Echo {
            value: "hello".to_string(),
        };
        let result = cmd.execute(&backend);
        assert_eq!(result, RespFrame::BulkString(b"hello".into()));

        Ok(())
    }
}
