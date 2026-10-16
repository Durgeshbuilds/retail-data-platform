# Postgres 17 (Alpine) with pgvector compiled in.
#
# Built on the same Alpine image the database was created with, rather than
# switching to the Debian-based pgvector/pgvector image: musl and glibc sort text
# differently, so an existing data volume would need every text index rebuilt.
FROM postgres:17-alpine

ARG PGVECTOR_VERSION=v0.8.7

RUN apk add --no-cache --virtual .build-deps build-base git \
    && git clone --branch "$PGVECTOR_VERSION" --depth 1 \
        https://github.com/pgvector/pgvector.git /tmp/pgvector \
    && cd /tmp/pgvector \
    # with_llvm=no: skip JIT bitcode, which would need clang/llvm in the image.
    && make with_llvm=no OPTFLAGS="" \
    && make with_llvm=no install \
    && cd / && rm -rf /tmp/pgvector \
    && apk del .build-deps
