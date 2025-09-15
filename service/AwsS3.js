import {
  S3Client,
  CreateBucketCommand,
  HeadBucketCommand,
  DeleteBucketCommand,
  PutObjectCommand,
  HeadObjectCommand,
  DeleteObjectCommand,
  CreateMultipartUploadCommand,
  UploadPartCommand,
  CompleteMultipartUploadCommand,
  AbortMultipartUploadCommand,
  ListObjectsV2Command,
  ListBucketsCommand,
  GetObjectCommand,
  CopyObjectCommand,
  DeleteObjectsCommand,
} from "@aws-sdk/client-s3";

import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import dotenv from "dotenv";
dotenv.config();

import SafeUtils from "../utils/SafeUtils.js";
import ErrorHandler from "../utils/ErrorHandler.js";
import Logger from "../utils/UtilityLogger.js";
import DateTime from "./DateTime.js"; // adjust path if needed

class AwsS3 {
  static client = null;
  static cache = { buckets: new Map(), objects: new Map() };

  static init(region) {
    try {
      ({ region } = SafeUtils.sanitizeValidate({
        region: { value: region, type: "string", required: true },
      }));
    } catch (err) {
      ErrorHandler.add_error("Invalid region in AwsS3.init", {
        region,
        error: err.message,
      });
      Logger.writeLog({
        flag: "system_error",
        action: "AwsS3.init",
        message: err.message,
        critical: true,
        data: { region },
      });
      throw new Error(err.message);
    }

    if (!AwsS3.client) {
      AwsS3.client = new S3Client({
        region,
        credentials: {
          accessKeyId: process.env.S3_ACCESS_KEY,
          secretAccessKey: process.env.S3_SECRET_KEY,
        },
      });
    }

    if (Logger.isConsoleEnabled()) {
      console.log(
        `[Logger flag=AwsS3.init]`,
        JSON.stringify(
          { action: "init", region, time: DateTime.now() },
          null,
          2
        )
      );
    }
  }

  static async createBucket(bucket) {
    try {
      ({ bucket } = SafeUtils.sanitizeValidate({
        bucket: { value: bucket, type: "string", required: true },
      }));
    } catch (err) {
      ErrorHandler.add_error("Invalid bucket in createBucket", {
        bucket,
        error: err.message,
      });
      console.log("Error Handler error", ErrorHandler.errors);
      Logger.writeLog({
        flag: "system_error",
        action: "createBucket",
        message: err.message,
        critical: true,
        data: { bucket },
      });
      throw new Error(err.message);
    }

    try {
      await AwsS3.client.send(new CreateBucketCommand({ Bucket: bucket }));
      AwsS3.cache.buckets.set(bucket, true);

      if (Logger.isConsoleEnabled()) {
        console.log(
          `[Logger flag=createBucket]`,
          JSON.stringify(
            { action: "createBucket", bucket, time: DateTime.now() },
            null,
            2
          )
        );
      }
    } catch (err) {
      ErrorHandler.add_error("createBucket failed", {
        bucket,
        error: err.message,
      });
      if (Logger.isConsoleEnabled()) {
        console.log(
          `[Logger flag=createBucket]`,
          JSON.stringify(
            {
              action: "createBucket.error",
              error: err.message,
              time: DateTime.now(),
            },
            null,
            2
          )
        );
      }
      return null;
    }
  }

  static async listBuckets() {
    try {
      const res = await AwsS3.client.send(new ListBucketsCommand());
      res.Buckets?.forEach((b) => AwsS3.cache.buckets.set(b.Name, true));

      if (Logger.isConsoleEnabled()) {
        console.log(
          `[Logger flag=listBuckets]`,
          JSON.stringify(
            {
              action: "listBuckets",
              count: res.Buckets?.length ?? 0,
              time: DateTime.now(),
            },
            null,
            2
          )
        );
      }
      return res.Buckets || [];
    } catch (err) {
      ErrorHandler.add_error("listBuckets failed", { error: err.message });
      if (Logger.isConsoleEnabled()) {
        console.log(
          `[Logger flag=listBuckets]`,
          JSON.stringify(
            {
              action: "listBuckets.error",
              error: err.message,
              time: DateTime.now(),
            },
            null,
            2
          )
        );
      }
      return null;
    }
  }

  static async doesBucketExist(bucket) {
    try {
      ({ bucket } = SafeUtils.sanitizeValidate({
        bucket: { value: bucket, type: "string", required: true },
      }));
    } catch (err) {
      ErrorHandler.add_error("Invalid bucket in doesBucketExist", {
        bucket,
        error: err.message,
      });
      Logger.writeLog({
        flag: "system_error",
        action: "doesBucketExist",
        message: err.message,
        critical: true,
        data: { bucket },
      });
      throw new Error(err.message);
    }

    if (AwsS3.cache.buckets.has(bucket)) {
      return AwsS3.cache.buckets.get(bucket);
    }

    try {
      await AwsS3.client.send(new HeadBucketCommand({ Bucket: bucket }));
      AwsS3.cache.buckets.set(bucket, true);

      if (Logger.isConsoleEnabled()) {
        console.log(
          `[Logger flag=doesBucketExist]`,
          JSON.stringify(
            {
              action: "doesBucketExist",
              bucket,
              exists: true,
              time: DateTime.now(),
            },
            null,
            2
          )
        );
      }
      return true;
    } catch (err) {
      if (err.name === "NotFound" || err.$metadata?.httpStatusCode === 404) {
        AwsS3.cache.buckets.set(bucket, false);
        return false;
      }
      ErrorHandler.add_error("doesBucketExist failed", {
        bucket,
        error: err.message,
      });
      if (Logger.isConsoleEnabled()) {
        console.log(
          `[Logger flag=doesBucketExist]`,
          JSON.stringify(
            {
              action: "doesBucketExist.error",
              error: err.message,
              time: DateTime.now(),
            },
            null,
            2
          )
        );
      }
      return null;
    }
  }

  static async deleteBucket(bucket) {
    try {
      ({ bucket } = SafeUtils.sanitizeValidate({
        bucket: { value: bucket, type: "string", required: true },
      }));
    } catch (err) {
      ErrorHandler.add_error("Invalid bucket in deleteBucket", {
        bucket,
        error: err.message,
      });
      Logger.writeLog({
        flag: "system_error",
        action: "deleteBucket",
        message: err.message,
        critical: true,
        data: { bucket },
      });
      throw new Error(err.message);
    }

    try {
      await AwsS3.client.send(new DeleteBucketCommand({ Bucket: bucket }));
      AwsS3.cache.buckets.delete(bucket);

      if (Logger.isConsoleEnabled()) {
        console.log(
          `[Logger flag=deleteBucket]`,
          JSON.stringify(
            { action: "deleteBucket", bucket, time: DateTime.now() },
            null,
            2
          )
        );
      }
    } catch (err) {
      ErrorHandler.add_error("deleteBucket failed", {
        bucket,
        error: err.message,
      });
      if (Logger.isConsoleEnabled()) {
        console.log(
          `[Logger flag=deleteBucket]`,
          JSON.stringify(
            {
              action: "deleteBucket.error",
              error: err.message,
              time: DateTime.now(),
            },
            null,
            2
          )
        );
      }
      return null;
    }
  }

  static async uploadFile(
    bucket,
    key,
    body,
    contentType = "application/octet-stream"
  ) {
    try {
      ({ bucket, key, contentType } = SafeUtils.sanitizeValidate({
        bucket: { value: bucket, type: "string", required: true },
        key: { value: key, type: "string", required: true },
        contentType: {
          value: contentType,
          type: "string",
          required: false,
          default: "application/octet-stream",
        },
      }));
    } catch (err) {
      ErrorHandler.add_error("Invalid params in uploadFile", {
        bucket,
        key,
        contentType,
        error: err.message,
      });
      Logger.writeLog({
        flag: "system_error",
        action: "uploadFile",
        message: err.message,
        critical: true,
        data: { bucket, key, contentType },
      });
      throw new Error(err.message);
    }

    try {
      await AwsS3.client.send(
        new PutObjectCommand({
          Bucket: bucket,
          Key: key,
          Body: body,
          ContentType: contentType,
        })
      );
      AwsS3.cache.objects.set(`${bucket}/${key}`, true);

      if (Logger.isConsoleEnabled()) {
        console.log(
          `[Logger flag=uploadFile]`,
          JSON.stringify(
            { action: "uploadFile", bucket, key, time: DateTime.now() },
            null,
            2
          )
        );
      }
    } catch (err) {
      ErrorHandler.add_error("uploadFile failed", {
        bucket,
        key,
        error: err.message,
      });
      if (Logger.isConsoleEnabled()) {
        console.log(
          `[Logger flag=uploadFile]`,
          JSON.stringify(
            {
              action: "uploadFile.error",
              error: err.message,
              time: DateTime.now(),
            },
            null,
            2
          )
        );
      }
      return null;
    }
  }

  static async doesFileExist(bucket, key) {
    try {
      ({ bucket, key } = SafeUtils.sanitizeValidate({
        bucket: { value: bucket, type: "string", required: true },
        key: { value: key, type: "string", required: true },
      }));
    } catch (err) {
      ErrorHandler.add_error("Invalid params in doesFileExist", {
        bucket,
        key,
        error: err.message,
      });
      Logger.writeLog({
        flag: "system_error",
        action: "doesFileExist",
        message: err.message,
        critical: true,
        data: { bucket, key },
      });
      throw new Error(err.message);
    }

    const objKey = `${bucket}/${key}`;
    if (AwsS3.cache.objects.has(objKey)) {
      return AwsS3.cache.objects.get(objKey);
    }

    try {
      await AwsS3.client.send(
        new HeadObjectCommand({ Bucket: bucket, Key: key })
      );
      AwsS3.cache.objects.set(objKey, true);

      if (Logger.isConsoleEnabled()) {
        console.log(
          `[Logger flag=doesFileExist]`,
          JSON.stringify(
            {
              action: "doesFileExist",
              key: objKey,
              exists: true,
              time: DateTime.now(),
            },
            null,
            2
          )
        );
      }
      return true;
    } catch (err) {
      if (err.name === "NotFound" || err.$metadata?.httpStatusCode === 404) {
        AwsS3.cache.objects.set(objKey, false);
        return false;
      }
      ErrorHandler.add_error("doesFileExist failed", {
        key: objKey,
        error: err.message,
      });
      if (Logger.isConsoleEnabled()) {
        console.log(
          `[Logger flag=doesFileExist]`,
          JSON.stringify(
            {
              action: "doesFileExist.error",
              error: err.message,
              time: DateTime.now(),
            },
            null,
            2
          )
        );
      }
      return null;
    }
  }

  static async deleteFile(bucket, key) {
    try {
      ({ bucket, key } = SafeUtils.sanitizeValidate({
        bucket: { value: bucket, type: "string", required: true },
        key: { value: key, type: "string", required: true },
      }));
    } catch (err) {
      ErrorHandler.add_error("Invalid params in deleteFile", {
        bucket,
        key,
        error: err.message,
      });
      Logger.writeLog({
        flag: "system_error",
        action: "deleteFile",
        message: err.message,
        critical: true,
        data: { bucket, key },
      });
      throw new Error(err.message);
    }

    try {
      await AwsS3.client.send(
        new DeleteObjectCommand({ Bucket: bucket, Key: key })
      );
      AwsS3.cache.objects.delete(`${bucket}/${key}`);

      if (Logger.isConsoleEnabled()) {
        console.log(
          `[Logger flag=deleteFile]`,
          JSON.stringify(
            {
              action: "deleteFile",
              key: `${bucket}/${key}`,
              time: DateTime.now(),
            },
            null,
            2
          )
        );
      }
    } catch (err) {
      ErrorHandler.add_error("deleteFile failed", {
        bucket,
        key,
        error: err.message,
      });
      if (Logger.isConsoleEnabled()) {
        console.log(
          `[Logger flag=deleteFile]`,
          JSON.stringify(
            {
              action: "deleteFile.error",
              error: err.message,
              time: DateTime.now(),
            },
            null,
            2
          )
        );
      }
      return null;
    }
  }

  static async deleteFiles(bucket, keys = []) {
    try {
      ({ bucket, keys } = SafeUtils.sanitizeValidate({
        bucket: { value: bucket, type: "string", required: true },
        keys: { value: keys, type: "array", required: true },
      }));
    } catch (err) {
      ErrorHandler.add_error("Invalid params in deleteFiles", {
        bucket,
        keys,
        error: err.message,
      });
      Logger.writeLog({
        flag: "system_error",
        action: "deleteFiles",
        message: err.message,
        critical: true,
        data: { bucket, keys },
      });
      throw new Error(err.message);
    }

    if (keys.length === 0) {
      ErrorHandler.add_error("deleteFiles called with empty keys array", {
        bucket,
      });
      return null;
    }

    try {
      const objects = keys.map((k) => ({ Key: k }));
      await AwsS3.client.send(
        new DeleteObjectsCommand({
          Bucket: bucket,
          Delete: { Objects: objects },
        })
      );
      keys.forEach((k) => AwsS3.cache.objects.delete(`${bucket}/${k}`));

      if (Logger.isConsoleEnabled()) {
        console.log(
          `[Logger flag=deleteFiles]`,
          JSON.stringify(
            {
              action: "deleteFiles",
              bucket,
              count: keys.length,
              time: DateTime.now(),
            },
            null,
            2
          )
        );
      }
    } catch (err) {
      ErrorHandler.add_error("deleteFiles failed", {
        bucket,
        keys,
        error: err.message,
      });
      if (Logger.isConsoleEnabled()) {
        console.log(
          `[Logger flag=deleteFiles]`,
          JSON.stringify(
            {
              action: "deleteFiles.error",
              error: err.message,
              time: DateTime.now(),
            },
            null,
            2
          )
        );
      }
      return null;
    }
  }

  static async listFiles(bucket, prefix = "") {
    try {
      ({ bucket, prefix } = SafeUtils.sanitizeValidate({
        bucket: { value: bucket, type: "string", required: true },
        prefix: { value: prefix, type: "string", required: false, default: "" },
      }));
    } catch (err) {
      ErrorHandler.add_error("Invalid params in listFiles", {
        bucket,
        prefix,
        error: err.message,
      });
      Logger.writeLog({
        flag: "system_error",
        action: "listFiles",
        message: err.message,
        critical: true,
        data: { bucket, prefix },
      });
      throw new Error(err.message);
    }

    try {
      const res = await AwsS3.client.send(
        new ListObjectsV2Command({
          Bucket: bucket,
          Prefix: prefix,
        })
      );
      if (Logger.isConsoleEnabled()) {
        console.log(
          `[Logger flag=listFiles]`,
          JSON.stringify(
            {
              action: "listFiles",
              bucket,
              count: res.Contents?.length ?? 0,
              time: DateTime.now(),
            },
            null,
            2
          )
        );
      }
      return res.Contents || [];
    } catch (err) {
      ErrorHandler.add_error("listFiles failed", {
        bucket,
        prefix,
        error: err.message,
      });
      if (Logger.isConsoleEnabled()) {
        console.log(
          `[Logger flag=listFiles]`,
          JSON.stringify(
            {
              action: "listFiles.error",
              error: err.message,
              time: DateTime.now(),
            },
            null,
            2
          )
        );
      }
      return null;
    }
  }

  static async getFile(bucket, key) {
    try {
      ({ bucket, key } = SafeUtils.sanitizeValidate({
        bucket: { value: bucket, type: "string", required: true },
        key: { value: key, type: "string", required: true },
      }));
    } catch (err) {
      ErrorHandler.add_error("Invalid params in getFile", {
        bucket,
        key,
        error: err.message,
      });
      Logger.writeLog({
        flag: "system_error",
        action: "getFile",
        message: err.message,
        critical: true,
        data: { bucket, key },
      });
      throw new Error(err.message);
    }

    try {
      const res = await AwsS3.client.send(
        new GetObjectCommand({ Bucket: bucket, Key: key })
      );
      if (Logger.isConsoleEnabled()) {
        console.log(
          `[Logger flag=getFile]`,
          JSON.stringify(
            {
              action: "getFile",
              key: `${bucket}/${key}`,
              time: DateTime.now(),
            },
            null,
            2
          )
        );
      }
      return res.Body;
    } catch (err) {
      ErrorHandler.add_error("getFile failed", {
        bucket,
        key,
        error: err.message,
      });
      if (Logger.isConsoleEnabled()) {
        console.log(
          `[Logger flag=getFile]`,
          JSON.stringify(
            {
              action: "getFile.error",
              error: err.message,
              time: DateTime.now(),
            },
            null,
            2
          )
        );
      }
      return null;
    }
  }

  static async copyFile(sourceBucket, sourceKey, destBucket, destKey) {
    try {
      ({ sourceBucket, sourceKey, destBucket, destKey } =
        SafeUtils.sanitizeValidate({
          sourceBucket: { value: sourceBucket, type: "string", required: true },
          sourceKey: { value: sourceKey, type: "string", required: true },
          destBucket: { value: destBucket, type: "string", required: true },
          destKey: { value: destKey, type: "string", required: true },
        }));
    } catch (err) {
      ErrorHandler.add_error("Invalid params in copyFile", {
        sourceBucket,
        sourceKey,
        destBucket,
        destKey,
        error: err.message,
      });
      Logger.writeLog({
        flag: "system_error",
        action: "copyFile",
        message: err.message,
        critical: true,
        data: { sourceBucket, sourceKey, destBucket, destKey },
      });
      throw new Error(err.message);
    }

    try {
      await AwsS3.client.send(
        new CopyObjectCommand({
          CopySource: `${sourceBucket}/${sourceKey}`,
          Bucket: destBucket,
          Key: destKey,
        })
      );
      AwsS3.cache.objects.set(`${destBucket}/${destKey}`, true);

      if (Logger.isConsoleEnabled()) {
        console.log(
          `[Logger flag=copyFile]`,
          JSON.stringify(
            {
              action: "copyFile",
              from: `${sourceBucket}/${sourceKey}`,
              to: `${destBucket}/${destKey}`,
              time: DateTime.now(),
            },
            null,
            2
          )
        );
      }
    } catch (err) {
      ErrorHandler.add_error("copyFile failed", {
        sourceBucket,
        sourceKey,
        destBucket,
        destKey,
        error: err.message,
      });
      if (Logger.isConsoleEnabled()) {
        console.log(
          `[Logger flag=copyFile]`,
          JSON.stringify(
            {
              action: "copyFile.error",
              error: err.message,
              time: DateTime.now(),
            },
            null,
            2
          )
        );
      }
      return null;
    }
  }

  static async initiateMultipartUpload(bucket, key) {
    try {
      ({ bucket, key } = SafeUtils.sanitizeValidate({
        bucket: { value: bucket, type: "string", required: true },
        key: { value: key, type: "string", required: true },
      }));
    } catch (err) {
      ErrorHandler.add_error("Invalid params in initiateMultipartUpload", {
        bucket,
        key,
        error: err.message,
      });
      Logger.writeLog({
        flag: "system_error",
        action: "initiateMultipartUpload",
        message: err.message,
        critical: true,
        data: { bucket, key },
      });
      throw new Error(err.message);
    }

    try {
      const res = await AwsS3.client.send(
        new CreateMultipartUploadCommand({
          Bucket: bucket,
          Key: key,
        })
      );
      if (Logger.isConsoleEnabled()) {
        console.log(
          `[Logger flag=initiateMultipartUpload]`,
          JSON.stringify(
            {
              action: "initiateMultipartUpload",
              key: `${bucket}/${key}`,
              uploadId: res.UploadId,
              time: DateTime.now(),
            },
            null,
            2
          )
        );
      }
      return res.UploadId;
    } catch (err) {
      ErrorHandler.add_error("initiateMultipartUpload failed", {
        bucket,
        key,
        error: err.message,
      });
      if (Logger.isConsoleEnabled()) {
        console.log(
          `[Logger flag=initiateMultipartUpload]`,
          JSON.stringify(
            {
              action: "initiateMultipartUpload.error",
              error: err.message,
              time: DateTime.now(),
            },
            null,
            2
          )
        );
      }
      return null;
    }
  }

  static async uploadPart(bucket, key, uploadId, partNumber, body) {
    try {
      ({ bucket, key, uploadId, partNumber } = SafeUtils.sanitizeValidate({
        bucket: { value: bucket, type: "string", required: true },
        key: { value: key, type: "string", required: true },
        uploadId: { value: uploadId, type: "string", required: true },
        partNumber: { value: partNumber, type: "int", required: true },
      }));
    } catch (err) {
      ErrorHandler.add_error("Invalid params in uploadPart", {
        bucket,
        key,
        uploadId,
        partNumber,
        error: err.message,
      });
      Logger.writeLog({
        flag: "system_error",
        action: "uploadPart",
        message: err.message,
        critical: true,
        data: { bucket, key, uploadId, partNumber },
      });
      throw new Error(err.message);
    }

    try {
      const res = await AwsS3.client.send(
        new UploadPartCommand({
          Bucket: bucket,
          Key: key,
          UploadId: uploadId,
          PartNumber: partNumber,
          Body: body,
        })
      );
      if (Logger.isConsoleEnabled()) {
        console.log(
          `[Logger flag=uploadPart]`,
          JSON.stringify(
            {
              action: "uploadPart",
              part: partNumber,
              ETag: res.ETag,
              time: DateTime.now(),
            },
            null,
            2
          )
        );
      }
      return { ETag: res.ETag, PartNumber: partNumber };
    } catch (err) {
      ErrorHandler.add_error("uploadPart failed", {
        bucket,
        key,
        uploadId,
        partNumber,
        error: err.message,
      });
      if (Logger.isConsoleEnabled()) {
        console.log(
          `[Logger flag=uploadPart]`,
          JSON.stringify(
            {
              action: "uploadPart.error",
              error: err.message,
              time: DateTime.now(),
            },
            null,
            2
          )
        );
      }
      return null;
    }
  }

  static async completeMultipartUpload(bucket, key, uploadId, parts) {
    try {
      ({ bucket, key, uploadId } = SafeUtils.sanitizeValidate({
        bucket: { value: bucket, type: "string", required: true },
        key: { value: key, type: "string", required: true },
        uploadId: { value: uploadId, type: "string", required: true },
      }));
    } catch (err) {
      ErrorHandler.add_error("Invalid params in completeMultipartUpload", {
        bucket,
        key,
        uploadId,
        error: err.message,
      });
      Logger.writeLog({
        flag: "system_error",
        action: "completeMultipartUpload",
        message: err.message,
        critical: true,
        data: { bucket, key, uploadId },
      });
      throw new Error(err.message);
    }

    try {
      await AwsS3.client.send(
        new CompleteMultipartUploadCommand({
          Bucket: bucket,
          Key: key,
          UploadId: uploadId,
          MultipartUpload: { Parts: parts },
        })
      );
      AwsS3.cache.objects.set(`${bucket}/${key}`, true);

      if (Logger.isConsoleEnabled()) {
        console.log(
          `[Logger flag=completeMultipartUpload]`,
          JSON.stringify(
            {
              action: "completeMultipartUpload",
              key: `${bucket}/${key}`,
              time: DateTime.now(),
            },
            null,
            2
          )
        );
      }
    } catch (err) {
      ErrorHandler.add_error("completeMultipartUpload failed", {
        bucket,
        key,
        uploadId,
        error: err.message,
      });
      if (Logger.isConsoleEnabled()) {
        console.log(
          `[Logger flag=completeMultipartUpload]`,
          JSON.stringify(
            {
              action: "completeMultipartUpload.error",
              error: err.message,
              time: DateTime.now(),
            },
            null,
            2
          )
        );
      }
      return null;
    }
  }

  static async abortMultipartUpload(bucket, key, uploadId) {
    try {
      ({ bucket, key, uploadId } = SafeUtils.sanitizeValidate({
        bucket: { value: bucket, type: "string", required: true },
        key: { value: key, type: "string", required: true },
        uploadId: { value: uploadId, type: "string", required: true },
      }));
    } catch (err) {
      ErrorHandler.add_error("Invalid params in abortMultipartUpload", {
        bucket,
        key,
        uploadId,
        error: err.message,
      });
      Logger.writeLog({
        flag: "system_error",
        action: "abortMultipartUpload",
        message: err.message,
        critical: true,
        data: { bucket, key, uploadId },
      });
      throw new Error(err.message);
    }

    try {
      await AwsS3.client.send(
        new AbortMultipartUploadCommand({
          Bucket: bucket,
          Key: key,
          UploadId: uploadId,
        })
      );

      if (Logger.isConsoleEnabled()) {
        console.log(
          `[Logger flag=abortMultipartUpload]`,
          JSON.stringify(
            {
              action: "abortMultipartUpload",
              key: `${bucket}/${key}`,
              uploadId,
              time: DateTime.now(),
            },
            null,
            2
          )
        );
      }
    } catch (err) {
      ErrorHandler.add_error("abortMultipartUpload failed", {
        bucket,
        key,
        uploadId,
        error: err.message,
      });
      if (Logger.isConsoleEnabled()) {
        console.log(
          `[Logger flag=abortMultipartUpload]`,
          JSON.stringify(
            {
              action: "abortMultipartUpload.error",
              error: err.message,
              time: DateTime.now(),
            },
            null,
            2
          )
        );
      }
      return null;
    }
  }

  static async getPresignedUrl(
    bucket,
    key,
    operation = "getObject",
    expiresInSeconds = 900
  ) {
    try {
      ({ bucket, key, operation, expiresInSeconds } =
        SafeUtils.sanitizeValidate({
          bucket: { value: bucket, type: "string", required: true },
          key: { value: key, type: "string", required: true },
          operation: {
            value: operation,
            type: "string",
            required: false,
            default: "getObject",
          },
          expiresInSeconds: {
            value: expiresInSeconds,
            type: "int",
            required: false,
            default: 900,
          },
        }));
    } catch (err) {
      ErrorHandler.add_error("Invalid params in getPresignedUrl", {
        bucket,
        key,
        operation,
        expiresInSeconds,
        error: err.message,
      });
      Logger.writeLog({
        flag: "system_error",
        action: "getPresignedUrl",
        message: err.message,
        critical: true,
        data: { bucket, key, operation, expiresInSeconds },
      });
      throw new Error(err.message);
    }

    let command;
    switch (operation) {
      case "getObject":
        command = new GetObjectCommand({ Bucket: bucket, Key: key });
        break;
      case "putObject":
        command = new PutObjectCommand({ Bucket: bucket, Key: key });
        break;
      default:
        ErrorHandler.add_error("Unsupported operation", { operation });
        Logger.writeLog({
          flag: "system_error",
          action: "getPresignedUrl",
          message: `Unsupported operation: ${operation}`,
          critical: true,
          data: { operation },
        });
        throw new Error(`Unsupported operation: ${operation}`);
    }

    try {
      const url = await getSignedUrl(AwsS3.client, command, {
        expiresIn: expiresInSeconds,
      });
      if (Logger.isConsoleEnabled()) {
        console.log(
          `[Logger flag=getPresignedUrl]`,
          JSON.stringify(
            {
              action: "getPresignedUrl",
              operation,
              expiresInSeconds,
              time: DateTime.now(),
            },
            null,
            2
          )
        );
      }
      return url;
    } catch (err) {
      ErrorHandler.add_error("getPresignedUrl failed", {
        bucket,
        key,
        operation,
        expiresInSeconds,
        error: err.message,
      });
      if (Logger.isConsoleEnabled()) {
        console.log(
          `[Logger flag=getPresignedUrl]`,
          JSON.stringify(
            {
              action: "getPresignedUrl.error",
              error: err.message,
              time: DateTime.now(),
            },
            null,
            2
          )
        );
      }
      return null;
    }
  }
}

export default AwsS3;
