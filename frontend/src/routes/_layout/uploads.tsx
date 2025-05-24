import { useCallback, useState } from 'react';
import {
  Container,
  Heading,
  Text,
  Box,
  VStack,
  Button,
  Flex,
  Center,
  Badge,
  HStack,
  Icon,
  Spinner,
} from "@chakra-ui/react";
import { createFileRoute } from "@tanstack/react-router";
import { useDropzone } from "react-dropzone";
import { FiUploadCloud, FiX, FiCheckCircle, FiAlertCircle, FiCpu } from "react-icons/fi";
import useCustomToast from "@/hooks/useCustomToast";
import { toaster } from "@/components/ui/toaster";
import { UploadsService, FilesService } from "@/client";
import type { PresignedUrlRequest, StartProcessRequest, InitiateMultipartRequest } from "@/client";

export const Route = createFileRoute("/_layout/uploads")({
  component: UploadsPage,
});

// Define a type for processing state
type ProcessingStatus = 'awaiting_action' | 'processing' | 'processed' | 'error';
interface FileProcessingState {
  status: ProcessingStatus;
  message?: string;
  fileId?: string; // Store the database file ID for processing
  uploadId?: string; // For multipart uploads
}

// Multipart upload threshold (100MB)
const MULTIPART_THRESHOLD = 100 * 1024 * 1024;
const MULTIPART_CHUNK_SIZE = 10 * 1024 * 1024; // 10MB chunks by default

function UploadsPage() {
  const [filesToUpload, setFilesToUpload] = useState<File[]>([]);
  const [uploadProgress, setUploadProgress] = useState<Record<string, number>>({});
  const [isUploadingOverall, setIsUploadingOverall] = useState(false);
  const [processingState, setProcessingState] = useState<Record<string, FileProcessingState>>({});
  const { showSuccessToast, showErrorToast } = useCustomToast();

  const onDrop = useCallback((acceptedFiles: File[]) => {
    const newFiles = acceptedFiles.filter(
      (newFile) => !filesToUpload.some((existingFile) => existingFile.name === newFile.name)
    );
    setFilesToUpload((prevFiles) => [...prevFiles, ...newFiles]);
  }, [filesToUpload]);

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    accept: {
      'audio/mpeg': ['.mp3'],
      'audio/wav': ['.wav'],
      'audio/aac': ['.aac'],
      'audio/ogg': ['.ogg'],
      'audio/flac': ['.flac'],
    },
  });

  const handleRemoveFile = (fileName: string) => {
    setFilesToUpload((prevFiles) =>
      prevFiles.filter((file) => file.name !== fileName),
    );
    setUploadProgress((prevProgress) => {
      const newProgress = { ...prevProgress };
      delete newProgress[fileName];
      return newProgress;
    });
    setProcessingState((prevProcessing) => {
      const newProcessing = { ...prevProcessing };
      delete newProcessing[fileName];
      return newProcessing;
    });
  };

  const handleSingleFileUpload = async (file: File) => {
    /**
     * Handles the upload of files smaller than MULTIPART_THRESHOLD using a single presigned URL
     */
    try {
      setUploadProgress((prev) => ({ ...prev, [file.name]: 0 }));

      // 1. Get presigned URL from backend
      const presignedUrlRequest: PresignedUrlRequest = {
        filename: file.name,
        content_type: file.type || undefined,
      };

      const presignedResponse = await UploadsService.createPresignedUploadUrl({
        requestBody: presignedUrlRequest,
      });

      // 2. Upload file to MinIO using presigned URL
      const formData = new FormData();
      formData.append('file', file);

      // Use XMLHttpRequest for progress tracking
      await new Promise<void>((resolve, reject) => {
        const xhr = new XMLHttpRequest();

        xhr.upload.addEventListener('progress', (event) => {
          if (event.lengthComputable) {
            const progress = Math.round((event.loaded / event.total) * 100);
            setUploadProgress((prev) => ({
              ...prev,
              [file.name]: progress,
            }));
          }
        });

        xhr.addEventListener('load', () => {
          if (xhr.status >= 200 && xhr.status < 300) {
            setUploadProgress((prev) => ({
              ...prev,
              [file.name]: 100,
            }));
            resolve();
          } else {
            reject(new Error(`Upload failed with status ${xhr.status}`));
          }
        });

        xhr.addEventListener('error', () => {
          reject(new Error('Upload failed due to network error'));
        });

        xhr.open('PUT', presignedResponse.url);

        // Set headers from the presigned response
        Object.entries(presignedResponse.headers_to_set).forEach(([key, value]) => {
          xhr.setRequestHeader(key, value);
        });

        xhr.send(file);
      });

      // 3. Mark as awaiting action after successful upload
      setProcessingState(prev => ({
        ...prev,
        [file.name]: {
          status: 'awaiting_action',
          fileId: presignedResponse.file_id,
        }
      }));
      showSuccessToast(`Successfully uploaded ${file.name}. File ID: ${presignedResponse.file_id.slice(-8)}. Ready for processing.`);

    } catch (error) {
      console.error("Single file upload error:", error);
      const errorMessage = (error as any).message || "An unknown error occurred during upload.";
      setUploadProgress((prev) => ({
        ...prev,
        [file.name]: -1, // Indicate upload error
      }));
      setProcessingState(prev => ({
        ...prev,
        [file.name]: {
          status: 'error',
          message: `Upload failed: ${errorMessage}`
        }
      }));
      throw error;
    }
  };

  const handleMultipartFileUpload = async (file: File) => {
    /**
     * Handles the upload of files larger than MULTIPART_THRESHOLD using multipart upload
     */
    try {
      setUploadProgress((prev) => ({ ...prev, [file.name]: 0 }));

      // 1. Initiate multipart upload
      const initiateRequest: InitiateMultipartRequest = {
        filename: file.name,
        content_type: file.type || undefined,
        file_size_bytes: file.size,
      };

      const initiateResponse = await UploadsService.initiateMultipartUploadEndpoint({
        requestBody: initiateRequest,
      });

      const { upload_id, file_id, recommended_part_size, total_parts } = initiateResponse;

      setProcessingState(prev => ({
        ...prev,
        [file.name]: {
          status: 'processing' as ProcessingStatus,
          message: `Uploading ${total_parts} parts...`,
          fileId: file_id,
          uploadId: upload_id,
        }
      }));

      // 2. Upload parts
      const parts: Array<{ part_number: number; etag: string }> = [];
      const partSize = recommended_part_size || MULTIPART_CHUNK_SIZE;

      for (let partNumber = 1; partNumber <= total_parts; partNumber++) {
        const start = (partNumber - 1) * partSize;
        const end = Math.min(start + partSize, file.size);
        const chunk = file.slice(start, end);

        // Get presigned URL for this part
        const partUrlResponse = await UploadsService.getMultipartPartUrl({
          requestBody: {
            file_id,
            upload_id,
            part_number: partNumber,
          },
        });

        // Upload the part
        const response = await fetch(partUrlResponse.url, {
          method: 'PUT',
          body: chunk,
          headers: partUrlResponse.headers_to_set,
        });

        if (!response.ok) {
          throw new Error(`Failed to upload part ${partNumber}: ${response.statusText}`);
        }

        // Extract ETag from response headers
        const etag = response.headers.get('ETag') || '';
        parts.push({ part_number: partNumber, etag: etag.replace(/"/g, '') });

        // Update progress
        const overallProgress = Math.round((partNumber / total_parts) * 100);
        setUploadProgress((prev) => ({
          ...prev,
          [file.name]: overallProgress,
        }));
      }

      // 3. Complete multipart upload
      const completeResponse = await UploadsService.completeMultipartUploadEndpoint({
        requestBody: {
          file_id,
          upload_id,
          parts,
        },
      });

      setUploadProgress((prev) => ({
        ...prev,
        [file.name]: 100,
      }));

      setProcessingState(prev => ({
        ...prev,
        [file.name]: {
          status: 'awaiting_action',
          fileId: file_id,
        }
      }));

      showSuccessToast(`Successfully uploaded ${file.name} (multipart). File ID: ${file_id.slice(-8)}. ETag: ${completeResponse.etag.slice(-8)}. Ready for processing.`);

    } catch (error) {
      console.error("Multipart upload error:", error);
      const errorMessage = (error as any).message || "An unknown error occurred during multipart upload.";

      // Try to abort the multipart upload if we have an upload ID
      const uploadState = processingState[file.name];
      if (uploadState?.uploadId && uploadState?.fileId) {
        try {
          await UploadsService.abortMultipartUploadEndpoint({
            fileId: uploadState.fileId,
            uploadId: uploadState.uploadId,
          });
        } catch (abortError) {
          console.error("Failed to abort multipart upload:", abortError);
        }
      }

      setUploadProgress((prev) => ({
        ...prev,
        [file.name]: -1,
      }));
      setProcessingState(prev => ({
        ...prev,
        [file.name]: {
          status: 'error',
          message: `Upload failed: ${errorMessage}`
        }
      }));
      throw error;
    }
  };

  const handleIndividualFileUpload = async (file: File) => {
    /**
     * Determines whether to use single or multipart upload based on file size
     */
    if (file.size > MULTIPART_THRESHOLD) {
      toaster.create({
        title: "Large file detected",
        description: `${file.name} is larger than 100MB. Using multipart upload for better reliability.`,
        type: "info",
        duration: 5000,
      });
      return handleMultipartFileUpload(file);
    } else {
      return handleSingleFileUpload(file);
    }
  };

  const handleUploadAll = async () => {
    if (filesToUpload.length === 0) {
      showErrorToast("Please select files to upload.");
      return;
    }

    setIsUploadingOverall(true);

    for (const file of filesToUpload) {
      // Only upload if not already uploaded or in error state from upload
      if (uploadProgress[file.name] === undefined || (uploadProgress[file.name] < 100 && uploadProgress[file.name] !== -1) ) {
        try {
          setProcessingState(prev => ({ ...prev, [file.name]: { status: 'pending_upload' as any } })); // Temporary state
          await handleIndividualFileUpload(file);
        } catch (error) {
          console.error("Upload error:", error);
          const errorMessage = (error as any).message || "An unknown error occurred during upload.";
          showErrorToast(errorMessage);
          setUploadProgress((prev) => ({
            ...prev,
            [file.name]: -1, // Indicate upload error
          }));
          setProcessingState(prev => ({ ...prev, [file.name]: { status: 'error', message: `Upload failed: ${errorMessage}` } }));
        }
      }
    }
    setIsUploadingOverall(false);
  };

  const handleProcessFile = async (fileName: string) => {
    /**
     * Triggers processing (transcription) for a successfully uploaded file by:
     * 1. Validating that the file has a valid database file ID
     * 2. Calling the FilesService.startFileProcessing API
     * 3. Updating UI state to reflect processing status
     */
    const currentProcessingState = processingState[fileName];
    if (!currentProcessingState?.fileId) {
      showErrorToast(`Cannot process ${fileName}: File ID not available`);
      return;
    }

    setProcessingState(prev => ({ ...prev, [fileName]: { ...prev[fileName]!, status: 'processing' } }));
    showSuccessToast(`Starting transcription for ${fileName}...`);

    try {
      const processRequest: StartProcessRequest = {
        process_type: "transcription", // Default to transcription
        target_hardware: "cpu", // Default to CPU
      };

      const processResponse = await FilesService.startFileProcessing({
        fileId: currentProcessingState.fileId,
        requestBody: processRequest,
      });

      setProcessingState(prev => ({
        ...prev,
        [fileName]: {
          ...prev[fileName]!,
          status: 'processed',
          message: `Process ID: ${processResponse.id.slice(-8)}`
        }
      }));
      showSuccessToast(`${fileName} transcription started successfully! Process ID: ${processResponse.id.slice(-8)}`);
    } catch (error) {
      console.error(`Error processing ${fileName}:`, error);
      const errorMessage = (error as any).message || `Failed to process ${fileName}.`;
      showErrorToast(errorMessage);
      setProcessingState(prev => ({
        ...prev,
        [fileName]: {
          ...prev[fileName]!,
          status: 'error',
          message: errorMessage
        }
      }));
    }
  };

  // Helper to format file size
  const formatFileSize = (bytes: number): string => {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  };

  return (
    <Container maxW="full">
      <Heading size="lg" pt={12}>
        Upload Audio Files
      </Heading>

      <Box
        {...getRootProps()}
        border="2px dashed"
        borderColor={isDragActive ? "teal.300" : "gray.200"}
        borderRadius="lg"
        p={8}
        mt={6}
        mb={6}
        textAlign="center"
        cursor="pointer"
        transition="all 0.2s"
        bg={isDragActive ? "teal.50" : "transparent"}
        _hover={{ bg: isDragActive ? "teal.50" : "gray.50" }}
      >
        <input {...getInputProps()} />
        <Center flexDirection="column" h="150px">
          <Icon
            as={FiUploadCloud}
            boxSize={12}
            color={isDragActive ? "teal.500" : "gray.400"}
            mb={4}
          />
          <VStack gap={2}>
            <Text fontWeight="medium">
              {isDragActive ? "Drop your audio files here" : "Drag & drop audio files here"}
            </Text>
            <Text fontSize="sm" color="gray.500">
              or click to browse your files
            </Text>
            <HStack mt={2} gap={2}>
              <Badge colorScheme="teal">MP3</Badge>
              <Badge colorScheme="teal">WAV</Badge>
              <Badge colorScheme="teal">AAC</Badge>
              <Badge colorScheme="teal">OGG</Badge>
              <Badge colorScheme="teal">FLAC</Badge>
            </HStack>
            <Text fontSize="xs" color="gray.500" mt={2}>
              Files larger than 100MB will use multipart upload
            </Text>
          </VStack>
        </Center>
      </Box>

      {filesToUpload.length > 0 ? (
        <VStack gap={5} align="stretch">
          <Flex justify="space-between" align="center">
            <Heading size="md">Files to Upload ({filesToUpload.length})</Heading>
            <Button
              size="sm"
              variant="outline"
              colorScheme="teal"
              disabled={isUploadingOverall}
              onClick={() => {
                if (!isUploadingOverall) {
                  setFilesToUpload([]);
                  setUploadProgress({});
                  setProcessingState({});
                }
              }}
            >
              Clear All
            </Button>
          </Flex>

          <Box mt={4}>
            {filesToUpload.map((file) => {
              const currentUploadProgress = uploadProgress[file.name] || 0;
              const isUploadError = currentUploadProgress === -1;
              const isUploadComplete = currentUploadProgress === 100 && !isUploadError;
              const isLargeFile = file.size > MULTIPART_THRESHOLD;

              const currentProcessingState = processingState[file.name];

              return (
                <Box
                  key={file.name}
                  p={4}
                  mb={3}
                  borderWidth="1px"
                  borderRadius="md"
                  shadow="sm"
                  bg="white"
                  position="relative"
                >
                  <Flex justify="space-between" align="center" mb={2}>
                    <HStack gap={2}>
                      {isUploadComplete && currentProcessingState?.status === 'processed' && <Icon as={FiCheckCircle} color="green.500" />}
                      {isUploadError && <Icon as={FiAlertCircle} color="red.500" />}
                      {(currentProcessingState?.status === 'error' && !isUploadError) && <Icon as={FiAlertCircle} color="red.500" />}
                      {currentProcessingState?.status === 'processing' && <Spinner size="xs" color="blue.500" />}

                      <Text
                        fontWeight="medium"
                        truncate
                        maxW={{ base: "150px", md: "300px" }}
                        title={file.name}
                      >
                        {file.name}
                      </Text>
                      {isLargeFile && <Badge colorScheme="purple" size="sm">Large File</Badge>}
                    </HStack>
                    <Button
                      size="xs"
                      aria-label="Remove file"
                      variant="ghost"
                      colorScheme="gray"
                      onClick={() => handleRemoveFile(file.name)}
                      disabled={isUploadingOverall && currentUploadProgress > 0 && currentUploadProgress < 100 || currentProcessingState?.status === 'processing'}
                    >
                      <Icon as={FiX} />
                    </Button>
                  </Flex>

                  <Flex fontSize="xs" color="gray.500" justify="space-between" mt={1} mb={2}>
                    <Text>{formatFileSize(file.size)}</Text>
                    {currentUploadProgress > 0 && !isUploadError && currentUploadProgress < 100 && (
                      <Text>{currentUploadProgress}% Uploading{isLargeFile ? ' (multipart)' : ''}</Text>
                    )}
                    {isUploadComplete && <Text color="green.500">Uploaded</Text>}
                    {isUploadError && <Text color="red.500">Upload Failed</Text>}
                  </Flex>

                  {currentUploadProgress > 0 && currentUploadProgress < 100 && !isUploadError &&  (
                    <Box mt={1} w="full" h="2px" bg="gray.200" borderRadius="full" overflow="hidden">
                      <Box
                        h="100%"
                        w={`${currentUploadProgress}%`}
                        bg={isLargeFile ? "purple.500" : "teal.500"}
                        transition="width 0.2s"
                      />
                    </Box>
                  )}
                  {isUploadError && (
                     <Box mt={1} w="full" h="2px" bg="red.200" borderRadius="full" overflow="hidden">
                      <Box h="100%" w="100%" bg="red.500" />
                    </Box>
                  )}

                  {/* Processing Status and Actions */}
                  {isUploadComplete && currentProcessingState && (
                    <Box mt={3}>
                      {currentProcessingState.status === 'awaiting_action' && (
                        <HStack gap={2}>
                          <Button
                            size="sm"
                            colorScheme="blue"
                            onClick={() => handleProcessFile(file.name)}
                          >
                            <Icon as={FiCpu} mr={2} />
                            Start Transcription
                          </Button>
                          <Badge colorScheme="gray" size="sm">
                            File ID: {currentProcessingState.fileId?.slice(-8)}
                          </Badge>
                        </HStack>
                      )}
                      {currentProcessingState.status === 'processing' && currentProcessingState.message?.includes('parts') && (
                        <HStack gap={2}>
                          <Spinner size="xs" color="purple.500" />
                          <Text fontSize="sm" color="purple.500">{currentProcessingState.message}</Text>
                        </HStack>
                      )}
                      {currentProcessingState.status === 'processing' && !currentProcessingState.message?.includes('parts') && (
                        <HStack gap={2}>
                          <Spinner size="xs" color="blue.500" />
                          <Text fontSize="sm" color="blue.500">Processing transcription...</Text>
                        </HStack>
                      )}
                      {currentProcessingState.status === 'processed' && (
                        <HStack gap={2}>
                          <Icon as={FiCheckCircle} color="green.500" />
                          <Text fontSize="sm" color="green.500">Transcription processing started!</Text>
                          {currentProcessingState.message && (
                            <Badge colorScheme="green" size="sm">
                              {currentProcessingState.message}
                            </Badge>
                          )}
                        </HStack>
                      )}
                      {currentProcessingState.status === 'error' && (
                        <HStack gap={2}>
                          <Icon as={FiAlertCircle} color="red.500" />
                          <Text fontSize="sm" color="red.500">
                            Processing Error: {currentProcessingState.message || "Unknown error"}
                          </Text>
                        </HStack>
                      )}
                    </Box>
                  )}
                </Box>
              );
            })}
          </Box>

          <Button
            colorScheme="teal"
            onClick={handleUploadAll}
            loading={isUploadingOverall}
            loadingText="Uploading Files..."
            disabled={isUploadingOverall || filesToUpload.every(f => uploadProgress[f.name] === 100 || uploadProgress[f.name] === -1) }
            size="md"
            mt={2}
          >
            Upload All Pending Files
          </Button>
        </VStack>
      ) : (
        <Box
          textAlign="center"
          p={8}
          mt={4}
          borderWidth="1px"
          borderRadius="md"
          borderColor="gray.200"
        >
          <Text color="gray.500">No files selected for upload.</Text>
        </Box>
      )}
    </Container>
  );
}

export default UploadsPage;
