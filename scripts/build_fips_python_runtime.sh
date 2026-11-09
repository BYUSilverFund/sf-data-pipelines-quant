#!/usr/bin/env bash
set -euo pipefail

python_version="3.13.18"
python_prefix="/home/gunnjake/.local/opt/python-${python_version}-fips"
build_root="/tmp/python-${python_version}-fips-build"
archive_path="${build_root}/Python-${python_version}.tar.xz"
archive_url="https://www.python.org/ftp/python/${python_version}/Python-${python_version}.tar.xz"
archive_sha256="abb0102e6d4ae027509bcb82327d9dd85515b44a3ba8ad538cfc5e9847029f68"

if [[ -e "${python_prefix}" ]]; then
    echo "Refusing to overwrite existing runtime: ${python_prefix}" >&2
    exit 2
fi

mkdir -p "${build_root}" "$(dirname "${python_prefix}")"
curl --fail --location --silent --show-error "${archive_url}" --output "${archive_path}"
printf '%s  %s\n' "${archive_sha256}" "${archive_path}" | sha256sum --check --status
tar --extract --xz --file "${archive_path}" --directory "${build_root}"

cd "${build_root}/Python-${python_version}"
./configure \
    --prefix="${python_prefix}" \
    --with-openssl=/usr \
    --with-openssl-rpath=auto \
    --with-ensurepip=install
make -j4
make install

"${python_prefix}/bin/python3.13" - <<'PY'
import ssl

context = ssl.create_default_context()
assert context.check_hostname
assert context.verify_mode == ssl.CERT_REQUIRED
print(ssl.OPENSSL_VERSION)
print("certificate verification enabled")
PY

UV_PROJECT_ENVIRONMENT=.venv uv sync --python "${python_prefix}/bin/python3.13"
UV_PROJECT_ENVIRONMENT=.venv-fips uv sync --python "${python_prefix}/bin/python3.13"
