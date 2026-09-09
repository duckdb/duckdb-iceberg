#include "storage/remote_signing/iceberg_remote_signed_file_system.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar/strftime_format.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

#include "storage/remote_signing/iceberg_remote_signer.hpp"

#include <cstring>

namespace duckdb {

IcebergRemoteSignedFileHandle::IcebergRemoteSignedFileHandle(IcebergRemoteSignedFileSystem &fs,
                                                             const OpenFileInfo &file, FileOpenFlags flags,
                                                             IcebergRemoteSigningTarget target_p, string http_url_p,
                                                             string host_p, weak_ptr<ClientContext> context_p)
    : FileHandle(fs, file.path, flags), target(std::move(target_p)), http_url(std::move(http_url_p)),
      host(std::move(host_p)), context(std::move(context_p)), last_modified(timestamp_t(0)) {
}

static bool IsS3Path(const string &path) {
	return StringUtil::StartsWith(path, "s3://") || StringUtil::StartsWith(path, "s3a://") ||
	       StringUtil::StartsWith(path, "s3n://");
}

bool IcebergRemoteSignedFileSystem::CanHandleFile(const string &fpath) {
	if (!IsS3Path(fpath)) {
		return false;
	}
	IcebergRemoteSigningTarget target;
	return registry->TryGetTarget(fpath, target);
}

static ClientContext &GetContext(IcebergRemoteSignedFileHandle &handle) {
	auto context = handle.context.lock();
	if (!context) {
		throw IOException("Cannot access '%s': the client context it was opened with is gone, so the Iceberg catalog "
		                  "can no longer be asked to sign requests for it",
		                  handle.path);
	}
	return *context;
}

using SignedRequestCallback =
    std::function<unique_ptr<HTTPResponse>(HTTPUtil &, const string &, HTTPHeaders &, HTTPParams &)>;

static unique_ptr<HTTPResponse> RunSignedRequest(IcebergRemoteSigningRegistry &registry,
                                                 IcebergRemoteSignedFileHandle &handle, RequestType request_type,
                                                 const HTTPHeaders &extra_headers,
                                                 const SignedRequestCallback &callback) {
	auto &context = GetContext(handle);
	auto &http_util = HTTPUtil::Get(*context.db);

	auto signature =
	    IcebergRemoteSigner::Sign(context, registry, handle.target, request_type, handle.http_url, handle.host);
	auto headers = signature.headers;
	for (auto &entry : extra_headers) {
		headers.Insert(entry.first, entry.second);
	}
	auto params = http_util.InitializeParameters(context, signature.url);
	return callback(http_util, signature.url, headers, *params);
}

static void CheckResponse(optional_ptr<HTTPResponse> response, const string &path, const char *operation) {
	if (!response) {
		throw IOException("Iceberg remote signed request while %s '%s' did not return a response", operation, path);
	}
	if (response->HasRequestError()) {
		throw IOException("Iceberg remote signed request while %s '%s' failed: %s", operation, path,
		                  response->GetRequestError());
	}
	if (response->status != HTTPStatusCode::OK_200 && response->status != HTTPStatusCode::PartialContent_206) {
		throw HTTPException(*response, "Iceberg remote signed request while %s '%s' returned status %d: %s", operation,
		                    path, static_cast<int>(response->status), response->body);
	}
}

void IcebergRemoteSignedFileSystem::DownloadFully(IcebergRemoteSignedFileHandle &handle) {
	auto response =
	    RunSignedRequest(*registry, handle, RequestType::GET_REQUEST, HTTPHeaders(),
	                     [](HTTPUtil &http_util, const string &url, HTTPHeaders &headers, HTTPParams &params) {
		                     GetRequestInfo get_request(url, headers, params, nullptr, nullptr);
		                     unique_ptr<HTTPClient> client;
		                     return http_util.Request(get_request, client);
	                     });
	CheckResponse(response.get(), handle.path, "downloading");
	handle.body = std::move(response->body);
	handle.length = handle.body.size();
	handle.fully_downloaded = true;
}

unique_ptr<FileHandle> IcebergRemoteSignedFileSystem::OpenFileExtended(const OpenFileInfo &file, FileOpenFlags flags,
                                                                       optional_ptr<FileOpener> opener) {
	if (flags.OpenForWriting()) {
		throw NotImplementedException(
		    "Cannot write to '%s': the Iceberg REST catalog delegates access to this location through remote request "
		    "signing, which the iceberg extension only implements for reads",
		    file.path);
	}
	auto context = FileOpener::TryGetClientContext(opener);
	if (!context) {
		throw IOException("Cannot open '%s': no client context is available to sign the request with", file.path);
	}
	IcebergRemoteSigningTarget target;
	if (!registry->TryGetTarget(file.path, target)) {
		throw IOException("No Iceberg remote signing configuration is registered for '%s'", file.path);
	}

	string host;
	auto http_url = IcebergRemoteSigner::ToHttpUrl(target, file.path, host);
	auto handle = make_uniq<IcebergRemoteSignedFileHandle>(*this, file, flags, std::move(target), std::move(http_url),
	                                                       std::move(host), context->shared_from_this());

	if (file.extended_info) {
		auto &options = file.extended_info->options;
		auto etag = options.find("etag");
		if (etag != options.end()) {
			handle->etag = StringValue::Get(etag->second);
		}
		auto force_full_download = options.find("force_full_download");
		if (force_full_download != options.end() && force_full_download->second.GetValue<bool>()) {
			DownloadFully(*handle);
			return std::move(handle);
		}
		//! Iceberg records the size of the files it points at, which lets the HEAD request be skipped
		auto file_size = options.find("file_size");
		if (file_size != options.end()) {
			handle->length = NumericCast<idx_t>(file_size->second.GetValue<uint64_t>());
			return std::move(handle);
		}
	}

	auto response =
	    RunSignedRequest(*registry, *handle, RequestType::HEAD_REQUEST, HTTPHeaders(),
	                     [](HTTPUtil &http_util, const string &url, HTTPHeaders &headers, HTTPParams &params) {
		                     HeadRequestInfo head_request(url, headers, params);
		                     unique_ptr<HTTPClient> client;
		                     return http_util.Request(head_request, client);
	                     });
	CheckResponse(response.get(), file.path, "opening");

	if (response->HasHeader("Content-Length")) {
		auto content_length = response->GetHeaderValue("Content-Length");
		uint64_t length = 0;
		if (!TryCast::Operation<string_t, uint64_t>(string_t(content_length), length)) {
			throw IOException("Iceberg remote signed HEAD request for '%s' returned an unparseable Content-Length "
			                  "header: %s",
			                  file.path, content_length);
		}
		handle->length = NumericCast<idx_t>(length);
	}
	if (response->HasHeader("ETag")) {
		handle->etag = response->GetHeaderValue("ETag");
	}
	if (response->HasHeader("Last-Modified")) {
		StrpTimeFormat::ParseResult parse_result;
		timestamp_t last_modified;
		if (StrpTimeFormat::TryParse("%a, %d %h %Y %T %Z", response->GetHeaderValue("Last-Modified"), parse_result) &&
		    parse_result.TryToTimestamp(last_modified)) {
			handle->last_modified = last_modified;
		}
	}
	return std::move(handle);
}

void IcebergRemoteSignedFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	if (nr_bytes == 0) {
		return;
	}
	auto &signed_handle = handle.Cast<IcebergRemoteSignedFileHandle>();
	auto buffer_out = static_cast<data_ptr_t>(buffer);
	auto buffer_out_len = NumericCast<idx_t>(nr_bytes);

	if (signed_handle.fully_downloaded) {
		if (location > signed_handle.length || buffer_out_len > signed_handle.length - location) {
			throw IOException("Cannot read %llu bytes at offset %llu from '%s', which is %llu bytes", buffer_out_len,
			                  location, signed_handle.path, signed_handle.length);
		}
		memcpy(buffer_out, signed_handle.body.data() + location, buffer_out_len);
		return;
	}

	HTTPHeaders range_header;
	range_header.Insert("Range", StringUtil::Format("bytes=%llu-%llu", location, location + buffer_out_len - 1));

	idx_t bytes_written = 0;
	bool collect_body = false;
	auto response = RunSignedRequest(
	    *registry, signed_handle, RequestType::GET_REQUEST, range_header,
	    [&](HTTPUtil &http_util, const string &url, HTTPHeaders &headers, HTTPParams &params) {
		    GetRequestInfo get_request(
		        url, headers, params,
		        [&](const HTTPResponse &response) {
			        //! A retried request restarts the body, so the output buffer restarts with it
			        bytes_written = 0;
			        collect_body = response.status == HTTPStatusCode::OK_200 ||
			                       response.status == HTTPStatusCode::PartialContent_206;
			        return true;
		        },
		        [&](const_data_ptr_t data, idx_t data_length) {
			        if (!collect_body) {
				        return true;
			        }
			        if (bytes_written + data_length > buffer_out_len) {
				        throw IOException("Iceberg remote signed range request for '%s' returned more than the "
				                          "requested %llu bytes",
				                          signed_handle.path, buffer_out_len);
			        }
			        memcpy(buffer_out + bytes_written, data, data_length);
			        bytes_written += data_length;
			        return true;
		        });
		    unique_ptr<HTTPClient> client;
		    return http_util.Request(get_request, client);
	    });
	CheckResponse(response.get(), signed_handle.path, "reading");

	if (bytes_written != buffer_out_len) {
		throw IOException("Iceberg remote signed range request for '%s' returned %llu bytes, expected %llu",
		                  signed_handle.path, bytes_written, buffer_out_len);
	}
}

int64_t IcebergRemoteSignedFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &signed_handle = handle.Cast<IcebergRemoteSignedFileHandle>();
	if (signed_handle.file_offset >= signed_handle.length) {
		return 0;
	}
	auto max_read = MinValue<idx_t>(NumericCast<idx_t>(nr_bytes), signed_handle.length - signed_handle.file_offset);
	Read(handle, buffer, NumericCast<int64_t>(max_read), signed_handle.file_offset);
	signed_handle.file_offset += max_read;
	return NumericCast<int64_t>(max_read);
}

int64_t IcebergRemoteSignedFileSystem::GetFileSize(FileHandle &handle) {
	return NumericCast<int64_t>(handle.Cast<IcebergRemoteSignedFileHandle>().length);
}

timestamp_t IcebergRemoteSignedFileSystem::GetLastModifiedTime(FileHandle &handle) {
	return handle.Cast<IcebergRemoteSignedFileHandle>().last_modified;
}

string IcebergRemoteSignedFileSystem::GetVersionTag(FileHandle &handle) {
	return handle.Cast<IcebergRemoteSignedFileHandle>().etag;
}

FileType IcebergRemoteSignedFileSystem::GetFileType(FileHandle &handle) {
	return FileType::FILE_TYPE_REGULAR;
}

FileMetadata IcebergRemoteSignedFileSystem::Stats(FileHandle &handle) {
	auto &signed_handle = handle.Cast<IcebergRemoteSignedFileHandle>();
	FileMetadata result;
	result.file_size = NumericCast<int64_t>(signed_handle.length);
	result.last_modification_time = signed_handle.last_modified;
	result.file_type = FileType::FILE_TYPE_REGULAR;
	return result;
}

bool IcebergRemoteSignedFileSystem::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	try {
		return OpenFile(filename, FileFlags::FILE_FLAGS_READ, opener) != nullptr;
	} catch (...) {
		return false;
	}
}

bool IcebergRemoteSignedFileSystem::DirectoryExists(const string &directory, optional_ptr<FileOpener> opener) {
	return true;
}

void IcebergRemoteSignedFileSystem::CreateDirectory(const string &directory, optional_ptr<FileOpener> opener) {
}

void IcebergRemoteSignedFileSystem::RemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
}

bool IcebergRemoteSignedFileSystem::TryRemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	return false;
}

void IcebergRemoteSignedFileSystem::RemoveFiles(const vector<string> &filenames, optional_ptr<FileOpener> opener) {
}

void IcebergRemoteSignedFileSystem::Seek(FileHandle &handle, idx_t location) {
	handle.Cast<IcebergRemoteSignedFileHandle>().file_offset = location;
}

idx_t IcebergRemoteSignedFileSystem::SeekPosition(FileHandle &handle) {
	return handle.Cast<IcebergRemoteSignedFileHandle>().file_offset;
}

void IcebergRemoteSignedFileSystem::Reset(FileHandle &handle) {
	handle.Cast<IcebergRemoteSignedFileHandle>().file_offset = 0;
}

} // namespace duckdb
