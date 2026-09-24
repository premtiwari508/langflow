"""What ``plan_file`` decides, and what it reads to decide it.

These run offline. The copy itself needs a real object store and lives in
``test_file_relocation.py``; the decision does not, and it is the part a caller
runs over every file before anything moves, so what it costs is worth pinning.

The two storage doubles below implement the ``StorageService`` methods the plan
uses and nothing else. Each records the reads it served, so a test can assert
that a file body was never opened.
"""

from __future__ import annotations

import hashlib

import pytest
from langflow.api.utils.file_relocation import plan_file
from langflow.services.storage.service import StorageService


class RecordingStorage(StorageService):
    """In-memory storage that counts body reads and can answer with or without a checksum."""

    def __init__(self, files: dict[str, bytes], *, knows_md5: bool) -> None:
        self.files = files
        self.knows_md5 = knows_md5
        self.bodies_read: list[str] = []

    def build_full_path(self, flow_id: str, file_name: str) -> str:
        return f"{flow_id}/{file_name}"

    def parse_file_path(self, full_path: str) -> tuple[str, str]:
        flow_id, _, file_name = full_path.partition("/")
        return flow_id, file_name

    async def get_file(self, flow_id: str, file_name: str) -> bytes:
        self.bodies_read.append(self.build_full_path(flow_id, file_name))
        return self.files[self.build_full_path(flow_id, file_name)]

    async def get_file_size(self, flow_id: str, file_name: str) -> int:
        key = self.build_full_path(flow_id, file_name)
        if key not in self.files:
            msg = f"File not found: {file_name}"
            raise FileNotFoundError(msg)
        return len(self.files[key])

    async def get_file_md5(self, flow_id: str, file_name: str) -> str | None:
        # A backend that would have to read the file to answer returns None, which is
        # what the base class does and what local disk keeps.
        if not self.knows_md5:
            return None
        key = self.build_full_path(flow_id, file_name)
        if key not in self.files:
            msg = f"File not found: {file_name}"
            raise FileNotFoundError(msg)
        return hashlib.md5(self.files[key]).hexdigest()  # noqa: S324 - stands in for an S3 ETag

    async def save_file(self, flow_id: str, file_name: str, data: bytes, *, append: bool = False) -> None:
        raise NotImplementedError

    def get_file_stream(self, flow_id: str, file_name: str, chunk_size: int = 8192):
        raise NotImplementedError

    async def list_files(self, flow_id: str) -> list[str]:
        raise NotImplementedError

    async def delete_file(self, flow_id: str, file_name: str) -> None:
        raise NotImplementedError

    async def teardown(self) -> None:
        return None


NAMESPACE = "3fa85f64-5717-4562-b3fc-2c963f66afa6"
NAME = "report.pdf"
KEY = f"{NAMESPACE}/{NAME}"


def _storage(content: bytes | None, *, knows_md5: bool) -> RecordingStorage:
    return RecordingStorage({KEY: content} if content is not None else {}, knows_md5=knows_md5)


class TestPlanReadsNoBodyWhenBothSidesCanAnswer:
    """Object storage on both sides settles this with two size checks and two checksums."""

    async def test_matching_content_is_skipped_without_reading_either_file(self):
        source = _storage(b"pdf-bytes", knows_md5=True)
        target = _storage(b"pdf-bytes", knows_md5=True)

        plan = await plan_file(source, target, NAMESPACE, NAME)

        assert plan.action == "skip"
        assert source.bodies_read == []
        assert target.bodies_read == []

    async def test_same_size_different_content_is_refused_without_reading_either_file(self):
        source = _storage(b"pdf-bytes", knows_md5=True)
        target = _storage(b"PDF-BYTES", knows_md5=True)

        plan = await plan_file(source, target, NAMESPACE, NAME)

        assert plan.action == "refuse"
        assert "different content" in (plan.reason or "")
        assert source.bodies_read == []
        assert target.bodies_read == []


class TestPlanFallsBackWhenASideCannotAnswer:
    async def test_a_source_without_a_checksum_is_read_and_still_decided(self):
        # Local disk: the only way to answer is to open the file.
        source = _storage(b"pdf-bytes", knows_md5=False)
        target = _storage(b"pdf-bytes", knows_md5=True)

        plan = await plan_file(source, target, NAMESPACE, NAME)

        assert plan.action == "skip"
        assert source.bodies_read == [KEY]

    async def test_a_source_without_a_checksum_still_catches_different_content(self):
        source = _storage(b"pdf-bytes", knows_md5=False)
        target = _storage(b"PDF-BYTES", knows_md5=True)

        plan = await plan_file(source, target, NAMESPACE, NAME)

        assert plan.action == "refuse"
        assert source.bodies_read == [KEY]

    async def test_a_target_without_a_checksum_settles_on_size_alone(self):
        source = _storage(b"pdf-bytes", knows_md5=True)
        target = _storage(b"PDF-BYTES", knows_md5=False)

        plan = await plan_file(source, target, NAMESPACE, NAME)

        assert plan.action == "skip"
        assert "size only" in (plan.reason or "")
        assert source.bodies_read == []


class TestPlanBeforeAnyChecksum:
    async def test_an_empty_target_is_a_copy_and_reads_nothing(self):
        source = _storage(b"pdf-bytes", knows_md5=True)
        target = _storage(None, knows_md5=True)

        plan = await plan_file(source, target, NAMESPACE, NAME)

        assert (plan.action, plan.size, plan.key) == ("copy", len(b"pdf-bytes"), KEY)
        assert source.bodies_read == []

    async def test_a_different_size_is_refused_and_reads_nothing(self):
        source = _storage(b"pdf-bytes", knows_md5=True)
        target = _storage(b"longer than nine", knows_md5=True)

        plan = await plan_file(source, target, NAMESPACE, NAME)

        assert plan.action == "refuse"
        assert "different size" in (plan.reason or "")
        assert source.bodies_read == []

    async def test_a_source_that_is_gone_is_reported_by_the_size_check(self):
        source = _storage(None, knows_md5=True)
        target = _storage(b"pdf-bytes", knows_md5=True)

        with pytest.raises(FileNotFoundError):
            await plan_file(source, target, NAMESPACE, NAME)
