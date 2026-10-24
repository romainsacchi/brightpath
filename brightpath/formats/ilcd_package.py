"""Offline ILCD 1.1 package indexing, secure XML parsing, and XSD validation."""

from __future__ import annotations

import hashlib
import json
import posixpath
import stat
import zipfile
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path, PurePosixPath
from urllib.parse import unquote, urlsplit
from uuid import UUID

from lxml import etree

from brightpath import DATA_DIR
from brightpath.core.reports import Issue, OperationKind, OperationReport, Severity, StageKind, StageReport
from brightpath.exceptions import SerializationError

BASE = "http://lca.jrc.it/ILCD/"
NS = {
    "p": BASE + "Process",
    "f": BASE + "Flow",
    "fp": BASE + "FlowProperty",
    "u": BASE + "UnitGroup",
    "c": BASE + "Common",
    "s": BASE + "Source",
    "ct": BASE + "Contact",
    "olca": "http://openlca.org/ilcd-extensions",
    "bp": "https://brightpath.dev/ilcd/1",
    "xml": "http://www.w3.org/XML/1998/namespace",
}
KINDS = {
    "processDataSet": ("Process", "processes"),
    "flowDataSet": ("Flow", "flows"),
    "flowPropertyDataSet": ("FlowProperty", "flowproperties"),
    "unitGroupDataSet": ("UnitGroup", "unitgroups"),
    "sourceDataSet": ("Source", "sources"),
    "contactDataSet": ("Contact", "contacts"),
    "LCIAMethodDataSet": ("LCIAMethod", "lciamethods"),
}
MAX_XML_BYTES = 16 * 1024 * 1024
MAX_PACKAGE_BYTES = 256 * 1024 * 1024
MAX_ENTRIES = 20_000
SCHEMA_DIRECTORY = DATA_DIR / "schemas" / "ilcd"


class ILCDError(SerializationError):
    """ILCD failure carrying a stable issue code and a serializable report."""

    def __init__(self, code: str, message: str, path: str = "", *, stage: StageKind = StageKind.SERIALIZATION):
        self.code, self.path = code, path
        super().__init__(
            message,
            report=OperationReport(
                OperationKind.READ if stage == StageKind.PARSE else OperationKind.WRITE,
                stages=(StageReport(stage, issues=(Issue(Severity.ERROR, code, message, stage, path=path),)),),
            ),
        )


def fail(code: str, message: str, path: str = "") -> None:
    raise ILCDError("ilcd." + code, message, path)


def q(prefix: str, name: str) -> str:
    return str(etree.QName(NS[prefix], name))


def xml_bytes(root: etree._Element) -> bytes:
    return etree.tostring(root, encoding="UTF-8", xml_declaration=True)


def parse_xml(contents: bytes) -> etree._Element:
    """Parse bounded XML without DTDs, entities, or network access."""
    if len(contents) > MAX_XML_BYTES:
        fail("xml_too_large", "ILCD XML exceeds the configured safety limit.")
    try:
        root = etree.fromstring(contents, etree.XMLParser(resolve_entities=False, no_network=True, load_dtd=False))
    except etree.XMLSyntaxError as error:
        fail("xml_invalid", str(error))
    if root.getroottree().docinfo.doctype:
        fail("doctype_forbidden", "ILCD XML must not declare a DTD.")
    return root


class _SchemaResolver(etree.Resolver):
    def resolve(self, url, public_id, context):
        if url in {"http://www.w3.org/2001/xml.xsd", "https://www.w3.org/2001/xml.xsd"}:
            return self.resolve_filename(str(SCHEMA_DIRECTORY / "ext" / "xml.xsd"), context)
        path = Path(url).resolve()
        if not path.is_relative_to(SCHEMA_DIRECTORY.resolve()):
            raise OSError("External schema resolution is disabled.")
        return self.resolve_filename(str(path), context)


@lru_cache(maxsize=8)
def schema(kind: str) -> etree.XMLSchema:
    """Load an integrity-checked packaged schema with local XML namespace imports."""
    manifest = json.loads((SCHEMA_DIRECTORY / "RESOURCE_MANIFEST.json").read_text())
    for resource in manifest["resources"]:
        path = SCHEMA_DIRECTORY / resource["path"]
        if hashlib.sha256(path.read_bytes()).hexdigest() != resource["sha256"]:
            fail("schema_integrity", f"ILCD schema does not match its manifest: {path.name}")
    parser = etree.XMLParser(resolve_entities=False, no_network=True, load_dtd=False)
    parser.resolvers.add(_SchemaResolver())
    filename = "ILCD_Locations.xsd" if kind == "Locations" else f"ILCD_{kind}DataSet.xsd"
    return etree.XMLSchema(etree.parse(str(SCHEMA_DIRECTORY / filename), parser))


def validate_xml(root: etree._Element) -> None:
    """Validate a supported ILCD entity against the pinned 1.1 schema."""
    local = etree.QName(root).localname
    if local not in KINDS or etree.QName(root).namespace != BASE + KINDS[local][0]:
        fail("root_unsupported", f"Unsupported ILCD root: {root.tag}")
    if root.get("version") != "1.1":
        fail("version_unsupported", "Only ILCD format version 1.1 is supported.")
    validator = schema(KINDS[local][0])
    if not validator.validate(root):
        fail("schema_invalid", "; ".join(str(error) for error in validator.error_log))


def uuid_text(value: object) -> str:
    try:
        return str(UUID(str(value)))
    except (ValueError, TypeError, AttributeError):
        fail("uuid_invalid", f"Invalid ILCD UUID: {value!r}")


def entity_key(root: etree._Element) -> tuple[str, str, str]:
    kind = KINDS[etree.QName(root).localname][0]
    return (
        kind,
        uuid_text(root.findtext(".//c:UUID", namespaces=NS)),
        root.findtext(".//c:dataSetVersion", namespaces=NS) or "",
    )


def _member_path(name: str) -> str:
    path = PurePosixPath(name)
    if "\\" in name or path.is_absolute() or ".." in path.parts or ":" in name or "\x00" in name:
        fail("archive_path_unsafe", f"Unsafe ILCD package member: {name!r}")
    return path.as_posix()


def read_members(source: str | Path) -> dict[str, bytes]:
    """Read XML resources from a ZIP, directory, or standalone XML file."""
    source = Path(source)
    result: dict[str, bytes] = {}
    total = 0
    if source.is_dir():
        paths = sorted(source.rglob("*"))
        if len(paths) > MAX_ENTRIES:
            fail("archive_too_large", "ILCD directory contains too many entries.")
        for path in paths:
            if path.is_symlink():
                fail("archive_path_unsafe", "ILCD package directories must not contain symlinks.")
            if path.is_file():
                total += path.stat().st_size
                if total > MAX_PACKAGE_BYTES:
                    fail("archive_too_large", "ILCD package exceeds the size limit.")
                if path.suffix.lower() == ".xml":
                    if path.stat().st_size > MAX_XML_BYTES:
                        fail("xml_too_large", "ILCD XML exceeds the size limit.")
                    result[path.relative_to(source).as_posix()] = path.read_bytes()
    elif zipfile.is_zipfile(source):
        with zipfile.ZipFile(source) as archive:
            infos = archive.infolist()
            if len(infos) > MAX_ENTRIES:
                fail("archive_too_large", "ILCD ZIP contains too many entries.")
            seen = set()
            for info in infos:
                name = _member_path(info.filename)
                if name in seen or stat.S_ISLNK(info.external_attr >> 16):
                    fail("archive_member_invalid", f"Duplicate or symlink ZIP member: {name}")
                seen.add(name)
                total += info.file_size
                if total > MAX_PACKAGE_BYTES or info.file_size > max(info.compress_size, 1) * 200:
                    fail("archive_too_large", "ILCD ZIP exceeds size or compression limits.")
                if name.lower().endswith(".xml") and not info.is_dir():
                    if info.file_size > MAX_XML_BYTES:
                        fail("xml_too_large", "ILCD XML exceeds the size limit.")
                    result[name] = archive.read(info)
    else:
        if not source.is_file() or source.stat().st_size > MAX_XML_BYTES:
            fail("artifact_invalid", "ILCD source is missing or exceeds the size limit.")
        result[source.name] = source.read_bytes()
    return result


def is_ilcd_archive(archive: zipfile.ZipFile) -> bool:
    """Probe an already-open ZIP for ILCD process XML without resolving links.

    This stream-safe boundary lets upload applications share BrightPath's
    namespace detection without materializing a second copy of the archive.
    """
    infos = archive.infolist()
    if len(infos) > MAX_ENTRIES or sum(i.file_size for i in infos) > MAX_PACKAGE_BYTES:
        fail("archive_too_large", "ILCD ZIP exceeds package limits.")
    seen = set()
    result = False
    for info in infos:
        name = _member_path(info.filename)
        if name in seen or stat.S_ISLNK(info.external_attr >> 16):
            fail("archive_member_invalid", f"Duplicate or symlink package member: {name}")
        seen.add(name)
        if info.file_size > max(info.compress_size, 1) * 200:
            fail("archive_too_large", "ILCD ZIP exceeds compression limits.")
        if PurePosixPath(name).parent.name == "processes" and name.lower().endswith(".xml"):
            if info.file_size > MAX_XML_BYTES:
                fail("xml_too_large", "ILCD process XML exceeds the safety limit.")
            root = parse_xml(archive.read(info))
            result |= root.tag == q("p", "processDataSet")
    return result


@dataclass
class ILCDPackage:
    """Indexed local XML entities, keyed by type, UUID, and exact dataset version."""

    resources: dict[str, str]
    entities: dict[tuple[str, str, str], tuple[str, etree._Element]]

    @classmethod
    def from_resources(cls, resources: dict[str, str | bytes], *, validate: bool = True) -> "ILCDPackage":
        for name in resources:
            _member_path(name)
        # A wrapper directory is packaging, not part of dataset identity. Strip
        # it consistently so snapshots from different archives can be combined.
        process_parents = {
            str(PurePosixPath(name).parent.parent)
            for name in resources
            if PurePosixPath(name).parent.name == "processes" and name.lower().endswith(".xml")
        }
        if len(process_parents) > 1:
            fail("package_root_ambiguous", "The package contains several process roots.")
        prefix = next(iter(process_parents), ".")
        if prefix != ".":
            marker = prefix + "/"
            resources = {name[len(marker) :]: content for name, content in resources.items() if name.startswith(marker)}
        entities = {}
        normalized = {}
        roots = set()
        for name, content in sorted(resources.items()):
            name = _member_path(name)
            root = parse_xml(content.encode() if isinstance(content, str) else content)
            local = etree.QName(root).localname
            if local in KINDS:
                if validate:
                    validate_xml(root)
                key = entity_key(root)
                if key in entities:
                    fail("reference_duplicate", f"Duplicate ILCD entity: {key}", name)
                entities[key] = (name, root)
                if key[0] == "Process" and PurePosixPath(name).parent.name == "processes":
                    roots.add(str(PurePosixPath(name).parent.parent))
            elif local in {"lifeCycleModelDataSet", "epdDataSet"}:
                fail("feature_unsupported", "eILCD lifecycle models and EPD datasets are not supported.", name)
            elif local == "ILCDLocations":
                validator = schema("Locations")
                if validate and not validator.validate(root):
                    fail("schema_invalid", "; ".join(str(e) for e in validator.error_log), name)
            normalized[name] = xml_bytes(root).decode()
        if len(roots) > 1:
            fail("package_root_ambiguous", "The archive contains several ILCD process package roots.")
        return cls(normalized, entities)

    def resolve(self, reference: etree._Element, owner: str) -> tuple[str, etree._Element]:
        """Resolve a typed reference; reject conflicting UUID/version/URI evidence."""
        kind = reference.get("type", "").removesuffix(" data set").replace(" ", "")
        identifier = uuid_text(reference.get("refObjectId"))
        version = reference.get("version")
        matches = [
            (path, root)
            for (k, uid, ver), (path, root) in self.entities.items()
            if k.lower() == kind.lower() and uid == identifier and (not version or ver == version)
        ]
        if len(matches) != 1:
            fail(
                "reference_missing" if not matches else "reference_ambiguous",
                f"Expected one local {kind} dataset for {identifier}, version {version or '(unspecified)' }.",
                owner,
            )
        uri = reference.get("uri", "")
        parsed = urlsplit(uri)
        if uri and not parsed.scheme and not parsed.netloc:
            path = posixpath.normpath(posixpath.join(posixpath.dirname(owner), unquote(parsed.path)))
            if path.startswith("../") or path.startswith("/") or "\\" in path:
                fail("reference_uri_unsafe", "Reference URI escapes the package.", owner)
            if path != matches[0][0]:
                fail("reference_conflict", "Reference URI disagrees with its UUID or version.", owner)
        return matches[0]

    def closure(self, path: str, root: etree._Element, *, findings: list | None = None) -> dict[str, str]:
        """Return this entity and all locally referenced auxiliary entities."""
        found = {}
        pending = [(path, root)]
        while pending:
            name, entity = pending.pop()
            if name in found:
                continue
            found[name] = self.resources[name]
            for reference in entity.xpath(".//*[@refObjectId]"):
                try:
                    pending.append(self.resolve(reference, name))
                except ILCDError as error:
                    if error.code != "ilcd.reference_missing" or reference.get("type") not in {
                        "source data set",
                        "contact data set",
                        "LCIA method data set",
                    }:
                        raise
                    if findings is not None:
                        finding = {
                            "code": "ilcd.auxiliary_reference_external",
                            "severity": "warning",
                            "message": f"Preserved external {reference.get('type')} reference {reference.get('refObjectId')}; its XML is not in the package.",
                            "path": name,
                        }
                        if finding not in findings:
                            findings.append(finding)
            locations = entity.get("locations")
            if locations and not urlsplit(locations).scheme:
                location_path = posixpath.normpath(posixpath.join(posixpath.dirname(name), locations))
                if location_path not in self.resources:
                    fail("reference_missing", f"Missing location resource: {locations}", name)
                found[location_path] = self.resources[location_path]
        return dict(sorted(found.items()))


def load_package(source: str | Path, *, reference_directory: str | Path | None = None) -> ILCDPackage:
    members = read_members(source)
    if reference_directory is not None:
        if len(members) == 1:
            name, contents = next(iter(members.items()))
            if parse_xml(contents).tag == q("p", "processDataSet") and PurePosixPath(name).parent.name != "processes":
                members = {"processes/" + PurePosixPath(name).name: contents}
        extra = read_members(reference_directory)
        for path, content in members.items():
            if path in extra and extra[path] != content:
                fail("reference_conflict", f"Conflicting resource: {path}")
        members = {**extra, **members}
    return ILCDPackage.from_resources(members)
