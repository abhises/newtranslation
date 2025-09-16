import express from "express";
import multer from "multer";
import AwsS3 from "../service/AwsS3.js";
import { ErrorHandler } from "../utils/index.js";

const router = express.Router();
const upload = multer(); // memory storage

// Set AWS region (do this once before any call)
AwsS3.init(process.env.AWS_REGION || "us-east-1");

// Upload file
router.post("/bucket", async (req, res) => {
  try {
    const { bucket } = req.body || {};
    const result = await AwsS3.createBucket(bucket);

    if (ErrorHandler.has_errors()) {
      const errors = ErrorHandler.get_all_errors();
      ErrorHandler.clear();
      return res.status(400).json({
        success: false,
        message: "Bucket creation failed",
        errors,
      });
    }

    return res.json({
      success: true,
      message: "Bucket created",
      result,
    });
  } catch (err) {
    const errors = ErrorHandler.get_all_errors();
    console.log("error in creating bucket", errors);
    ErrorHandler.clear();
    console.error("Caught unexpected error:", err.message);
    return res.status(500).json({
      message: "Unexpected error occurred",
      error: err.message,
      details: errors.length ? errors : undefined,
    });
  }
});

// ✅ List all buckets
router.get("/buckets", async (req, res) => {
  try {
    const buckets = await AwsS3.listBuckets();

    if (ErrorHandler.has_errors()) {
      const errors = ErrorHandler.get_all_errors();
      ErrorHandler.clear();
      return res.status(400).json({
        success: false,
        message: "Failed to list buckets",
        errors,
      });
    }

    return res.json({
      success: true,
      message: "Buckets fetched successfully",
      buckets,
    });
  } catch (err) {
    const errors = ErrorHandler.get_all_errors();
    ErrorHandler.clear();
    console.error("Caught unexpected error:", err.message);
    return res.status(500).json({
      success: false,
      message: "Unexpected error occurred",
      error: err.message,
      details: errors.length ? errors : undefined,
    });
  }
});

// ✅ Check if bucket exists
router.get("/bucket/exists", async (req, res) => {
  try {
    const { bucket } = req.query;
    const exists = await AwsS3.doesBucketExist(bucket);

    if (ErrorHandler.has_errors()) {
      const errors = ErrorHandler.get_all_errors();
      ErrorHandler.clear();
      return res.status(400).json({
        success: false,
        message: "Bucket existence check failed",
        errors,
      });
    }

    return res.json({
      success: true,
      message: `Bucket ${exists ? "exists" : "does not exist"}`,
      exists,
    });
  } catch (err) {
    const errors = ErrorHandler.get_all_errors();
    console.log(errors);
    ErrorHandler.clear();
    console.error("Caught unexpected error:", err.message);
    return res.status(500).json({
      success: false,
      message: "Unexpected error occurred",
      error: err.message,
      details: errors.length ? errors : undefined,
    });
  }
});

// ✅ Delete a bucket
router.delete("/bucket", async (req, res) => {
  try {
    const { bucket } = req.body;
    const result = await AwsS3.deleteBucket(bucket);
    if (ErrorHandler.has_errors()) {
      const errors = ErrorHandler.get_all_errors();
      ErrorHandler.clear();
      return res.status(400).json({
        success: false,
        message: "Bucket deletion failed",
        errors,
      });
    }

    return res.json({
      success: true,
      message: "Bucket deleted",
      result,
    });
  } catch (err) {
    const errors = ErrorHandler.get_all_errors();
    ErrorHandler.clear();
    return res.status(500).json({
      message: "Unexpected error occurred",
      error: err.message,
      details: errors.length ? errors : undefined,
    });
  }
});

// ✅ Upload file
router.post("/upload", upload.single("file"), async (req, res) => {
  try {
    const { bucket, key } = req.body;
    const file = req.file;

    const result = await AwsS3.uploadFile(
      bucket,
      key,
      file?.buffer,
      file?.mimetype
    );

    if (ErrorHandler.has_errors()) {
      const errors = ErrorHandler.get_all_errors();
      ErrorHandler.clear();
      return res.status(400).json({
        success: false,
        message: "Upload failed",
        errors,
      });
    }

    return res.json({
      success: true,
      message: "File uploaded successfully",
      result,
    });
  } catch (err) {
    const errors = ErrorHandler.get_all_errors();
    ErrorHandler.clear();
    console.error("Caught unexpected error:", err.message);
    return res.status(500).json({
      message: "Unexpected error occurred",
      error: err.message,
      details: errors.length ? errors : undefined,
    });
  }
});

// ✅ Check if a file exists
router.get("/file/exists", async (req, res) => {
  try {
    const { bucket, key } = req.query;
    if (!bucket || !key) {
      ErrorHandler.add_error("bucket and key are required", { bucket, key });
      const errors = ErrorHandler.get_all_errors();
      ErrorHandler.clear();
      return res.status(400).json({
        success: false,
        message: "Missing required query parameters",
        errors,
      });
    }

    const exists = await AwsS3.doesFileExist(bucket, key);
    if (ErrorHandler.has_errors()) {
      const errors = ErrorHandler.get_all_errors();
      ErrorHandler.clear();
      return res.status(400).json({
        success: false,
        message: "File existence check failed",
        errors,
      });
    }

    return res.json({ success: true, exists });
  } catch (err) {
    const errors = ErrorHandler.get_all_errors();
    ErrorHandler.clear();
    console.error("Caught error in doesFileExist route:", err.message);
    return res.status(500).json({
      success: false,
      message: "Unexpected error occurred",
      error: err.message,
      details: errors.length ? errors : undefined,
    });
  }
});

// ✅ Get file
router.get("/file", async (req, res) => {
  try {
    const { bucket, key } = req.query;
    const fileStream = await AwsS3.getFile(bucket, key);

    if (!fileStream) {
      const errors = ErrorHandler.get_all_errors();
      ErrorHandler.clear();
      return res.status(404).json({ success: false, errors });
    }

    ErrorHandler.clear();
    return res.status(200).json({
      success: true,
      message: `File "${key}" found in bucket "${bucket}"`,
    });
  } catch (err) {
    const errors = ErrorHandler.get_all_errors();
    ErrorHandler.clear();
    console.error("Caught error in /file route:", errors);
    return res.status(500).json({
      success: false,
      message: "Unexpected error occurred",
      error: errors,
      details: errors.length ? errors : undefined,
    });
  }
});

// ✅ Delete a file
router.delete("/file", async (req, res) => {
  try {
    const { bucket, key } = req.body;
    if (!bucket || !key) {
      return res.status(400).json({ error: "bucket and key are required" });
    }

    const result = await AwsS3.deleteFile(bucket, key);
    if (result === null) {
      const lastError = ErrorHandler.get_all_errors();
      return res.status(500).json({
        error: "Failed to delete file",
        details: lastError || null,
      });
    }

    res.json({ success: true, message: "File deleted" });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ✅ Delete multiple files
router.delete("/files", async (req, res) => {
  try {
    const { bucket, keys } = req.body;
    const result = await AwsS3.deleteFiles(bucket, keys);

    if (result === null) {
      const errors = ErrorHandler.get_all_errors();
      console.error("🛑 ErrorHandler errors from deleteFiles:", errors);
      return res.status(500).json({
        error: "Failed to delete files",
        details: errors,
      });
    }

    res.json({ success: true, message: "Files deleted successfully" });
    ErrorHandler.clear();
  } catch (err) {
    console.error("🔥 Unexpected router-level error:", err);
    return res.status(500).json({
      error: "Unexpected server error",
      details: [{ message: err.message }],
    });
  }
});

// ✅ List files
router.get("/files", async (req, res) => {
  try {
    const { bucket, prefix = "" } = req.query;
    if (!bucket) return res.status(400).json({ error: "bucket is required" });

    const files = await AwsS3.listFiles(bucket, prefix);
    res.json({ success: true, files });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ✅ Copy file
router.post("/file/copy", async (req, res) => {
  try {
    const { sourceBucket, sourceKey, destBucket, destKey } = req.body;
    await AwsS3.copyFile(sourceBucket, sourceKey, destBucket, destKey);

    if (ErrorHandler.has_errors()) {
      const errors = ErrorHandler.get_all_errors();
      ErrorHandler.clear();
      return res.status(500).json({
        success: false,
        message: "Failed to copy file",
        errors,
      });
    }

    return res.json({
      success: true,
      message: `File copied from "${sourceBucket}/${sourceKey}" to "${destBucket}/${destKey}"`,
    });
  } catch (err) {
    const errors = ErrorHandler.get_all_errors();
    console.log("errors", errors);
    ErrorHandler.clear();
    console.error("Caught error in file copy route:", err.message);
    return res.status(500).json({
      success: false,
      message: errors.message,
      error: errors,
      details: errors.length ? errors : errors,
    });
  }
});

// ✅ Multipart upload routes
router.post("/multipart/initiate", async (req, res) => {
  const { bucket, key } = req.body;
  if (!bucket || !key) {
    return res.status(400).json({ error: "bucket and key are required" });
  }

  try {
    const uploadId = await AwsS3.initiateMultipartUpload(bucket, key);
    res.json({ uploadId });
  } catch (err) {
    console.error("Error initiating multipart upload:", err);
    res.status(500).json({ error: err.message });
  }
});

router.post("/multipart/upload-part", async (req, res) => {
  const { bucket, key, uploadId, partNumber, bodyBase64 } = req.body;
  if (!bucket || !key || !uploadId || !partNumber || !bodyBase64) {
    return res.status(400).json({ error: "All fields are required" });
  }

  try {
    const bodyBuffer = Buffer.from(bodyBase64, "base64");
    const result = await AwsS3.uploadPart(
      bucket,
      key,
      uploadId,
      partNumber,
      bodyBuffer
    );
    res.json(result);
  } catch (err) {
    console.error("Error uploading part:", err);
    res.status(500).json({ error: err.message });
  }
});

router.post("/multipart/complete", async (req, res) => {
  try {
    const { bucket, key, uploadId, parts } = req.body;
    if (!bucket || !key || !uploadId || !parts || !Array.isArray(parts)) {
      return res.status(400).json({
        success: false,
        message: "bucket, key, uploadId and parts array are required",
      });
    }

    await AwsS3.completeMultipartUpload(bucket, key, uploadId, parts);

    if (ErrorHandler.has_errors()) {
      const errors = ErrorHandler.get_all_errors();
      ErrorHandler.clear();
      return res.status(400).json({
        success: false,
        message: "Failed to complete multipart upload",
        errors,
      });
    }

    return res.json({
      success: true,
      message: "Multipart upload completed successfully",
    });
  } catch (err) {
    const errors = ErrorHandler.get_all_errors();
    ErrorHandler.clear();
    return res.status(500).json({
      success: false,
      message: err.message || "Unexpected error",
      errors: errors.length ? errors : undefined,
    });
  }
});

router.post("/multipart/abort", async (req, res) => {
  try {
    const { bucket, key, uploadId } = req.body;
    await AwsS3.abortMultipartUpload(bucket, key, uploadId);

    if (ErrorHandler.has_errors()) {
      const errors = ErrorHandler.get_all_errors();
      ErrorHandler.clear();
      return res.status(500).json({
        success: false,
        message: "Failed to abort multipart upload",
        errors,
      });
    }

    return res.json({
      success: true,
      message: `Multipart upload aborted for "${bucket}/${key}"`,
    });
  } catch (err) {
    const errors = ErrorHandler.get_all_errors();
    ErrorHandler.clear();
    console.error("Caught error in abort route:", err.message);
    return res.status(500).json({
      success: false,
      message: "Unexpected error occurred during abort",
      error: err.message,
      details: errors.length ? errors : undefined,
    });
  }
});

// ✅ Presigned URL
router.get("/presign", async (req, res) => {
  try {
    const { bucket, key, op = "getObject" } = req.query;
    if (!bucket || !key)
      return res.status(400).json({ error: "bucket and key are required" });

    const url = await AwsS3.getPresignedUrl(bucket, key, op);
    res.json({ success: true, url });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

export default router;
