"""
Tests for the error classification and custom exceptions.
"""

import errno
import socket


from simpleetl.core.errors import (
    ETLError,
    ExtractError,
    TransformError,
    LoadError,
    PartialFailureError,
    ErrorClassification,
    classify_error,
)


class TestETLError:
    """Tests for the base ETLError exception."""

    def test_basic_message(self):
        err = ETLError("something went wrong")
        assert str(err) == "something went wrong"

    def test_with_job_name(self):
        err = ETLError("fail", job_name="my_job")
        assert "job=my_job" in str(err)

    def test_with_phase(self):
        err = ETLError("fail", phase="extract")
        assert "phase=extract" in str(err)

    def test_with_all_fields(self):
        err = ETLError("fail", job_name="my_job", phase="transform")
        result = str(err)
        assert "job=my_job" in result
        assert "phase=transform" in result

    def test_with_cause(self):
        cause = ValueError("original")
        err = ETLError("wrapped", cause=cause)
        assert "caused_by" in str(err)

    def test_record_info(self):
        err = ETLError("fail", record_info={"row": 5, "col": "name"})
        assert err.record_info == {"row": 5, "col": "name"}

    def test_timestamp_is_set(self):
        err = ETLError("fail")
        assert err.timestamp is not None


class TestPhaseErrors:
    """Tests for phase-specific exceptions."""

    def test_extract_error_phase(self):
        err = ExtractError("extract failed")
        assert err.phase == "extract"
        assert isinstance(err, ETLError)

    def test_transform_error_phase(self):
        err = TransformError("transform failed")
        assert err.phase == "transform"
        assert isinstance(err, ETLError)

    def test_load_error_phase(self):
        err = LoadError("load failed")
        assert err.phase == "load"
        assert isinstance(err, ETLError)

    def test_extract_error_with_job_name(self):
        err = ExtractError("fail", job_name="etl_job")
        assert err.job_name == "etl_job"
        assert err.phase == "extract"


class TestPartialFailureError:
    """Tests for PartialFailureError."""

    def test_basic(self):
        failed = [(0, "bad data"), (5, "missing field")]
        err = PartialFailureError("some records failed", failed)
        assert err.success_count == 0
        assert err.failure_count == 2
        assert len(err.failed_records) == 2

    def test_with_success_count(self):
        failed = [(3, "error")]
        err = PartialFailureError("partial", failed, success_count=9)
        assert err.success_count == 9
        assert err.failure_count == 1

    def test_str_representation(self):
        failed = [(0, "err")]
        err = PartialFailureError("msg", failed, success_count=5)
        result = str(err)
        assert "successes=5" in result
        assert "failures=1" in result


class TestClassifyError:
    """Tests for the classify_error function."""

    # Transient errors

    def test_connection_error_is_transient(self):
        assert classify_error(ConnectionError("conn refused")) == ErrorClassification.TRANSIENT

    def test_timeout_error_is_transient(self):
        assert classify_error(TimeoutError("timed out")) == ErrorClassification.TRANSIENT

    def test_socket_timeout_is_transient(self):
        assert classify_error(socket.timeout("timed out")) == ErrorClassification.TRANSIENT

    def test_broken_pipe_is_transient(self):
        assert classify_error(BrokenPipeError()) == ErrorClassification.TRANSIENT

    def test_connection_reset_is_transient(self):
        assert classify_error(ConnectionResetError()) == ErrorClassification.TRANSIENT

    def test_connection_refused_is_transient(self):
        assert classify_error(ConnectionRefusedError()) == ErrorClassification.TRANSIENT

    def test_interrupted_is_transient(self):
        assert classify_error(InterruptedError()) == ErrorClassification.TRANSIENT

    def test_os_error_generic_is_transient(self):
        err = OSError(errno.EIO, "I/O error")
        assert classify_error(err) == ErrorClassification.TRANSIENT

    # Permanent errors

    def test_file_not_found_is_permanent(self):
        assert classify_error(FileNotFoundError("no such file")) == ErrorClassification.PERMANENT

    def test_permission_error_is_permanent(self):
        assert classify_error(PermissionError("denied")) == ErrorClassification.PERMANENT

    def test_value_error_is_permanent(self):
        assert classify_error(ValueError("bad value")) == ErrorClassification.PERMANENT

    def test_type_error_is_permanent(self):
        assert classify_error(TypeError("wrong type")) == ErrorClassification.PERMANENT

    def test_key_error_is_permanent(self):
        assert classify_error(KeyError("missing_key")) == ErrorClassification.PERMANENT

    def test_os_error_noent_is_permanent(self):
        err = OSError(errno.ENOENT, "No such file")
        assert classify_error(err) == ErrorClassification.PERMANENT

    def test_os_error_eacces_is_permanent(self):
        err = OSError(errno.EACCES, "Permission denied")
        assert classify_error(err) == ErrorClassification.PERMANENT

    def test_os_error_eperm_is_permanent(self):
        err = OSError(errno.EPERM, "Operation not permitted")
        assert classify_error(err) == ErrorClassification.PERMANENT

    def test_os_error_eisdir_is_permanent(self):
        err = OSError(errno.EISDIR, "Is a directory")
        assert classify_error(err) == ErrorClassification.PERMANENT

    def test_os_error_einval_is_permanent(self):
        err = OSError(errno.EINVAL, "Invalid argument")
        assert classify_error(err) == ErrorClassification.PERMANENT

    # Unknown errors

    def test_runtime_error_is_unknown(self):
        assert classify_error(RuntimeError("oops")) == ErrorClassification.UNKNOWN

    def test_arithmetic_error_is_unknown(self):
        assert classify_error(ArithmeticError("div zero")) == ErrorClassification.UNKNOWN

    # Chained errors

    def test_chained_transient_cause(self):
        cause = ConnectionError("conn lost")
        err = RuntimeError("wrapper")
        err.__cause__ = cause
        assert classify_error(err) == ErrorClassification.TRANSIENT

    def test_chained_permanent_cause(self):
        cause = ValueError("bad schema")
        err = RuntimeError("wrapper")
        err.__cause__ = cause
        assert classify_error(err) == ErrorClassification.PERMANENT

    def test_implicit_context_permanent(self):
        err = RuntimeError("wrapper")
        err.__context__ = ValueError("original")
        err.__suppress_context__ = False
        assert classify_error(err) == ErrorClassification.PERMANENT

    def test_suppressed_context_not_followed(self):
        cause = ValueError("original")
        err = RuntimeError("wrapper")
        err.__context__ = cause
        err.__suppress_context__ = True
        assert classify_error(err) == ErrorClassification.UNKNOWN
