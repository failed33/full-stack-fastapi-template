/**
 * React hook for receiving **server‑sent events (SSE)** from the backend.
 * It keeps the API identical to the former WebSocket version so callers
 * only need to switch the URL and (optionally) the filename import.
 *
 * Auth: a JWT is appended as `?token=…` query param because `EventSource`
 *       does not allow setting custom headers.
 *
 * Re‑connect logic: exponential back‑off up to 30 s whenever the browser
 * reports the stream has closed (readyState === EventSource.CLOSED).
 */

import { useCallback, useEffect, useRef, useState } from "react";

export interface MinioObjectCreatedEvent {
  event_type: string;
  data: {
    object_key: string;
    bucket_name: string;
    object_size: number;
    user_id?: string;
    upload_uuid?: string;
    event_time: string;
  };
}

type SSEUpdateHandler = (message: MinioObjectCreatedEvent) => void;

const MAX_RETRY_DELAY = 30_000;   // 30 s
const INITIAL_RETRY_DELAY = 1_000; // 1 s

export const useSSEUpdates = () => {
  const [isConnected, setIsConnected] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const eventSource = useRef<EventSource | null>(null);
  const handlers = useRef<Record<string, SSEUpdateHandler[]>>({});
  const retryTimeoutRef = useRef<NodeJS.Timeout | null>(null);
  const retryAttempt = useRef(0);

  /* ---------- public API: registerHandler ---------- */
  const registerHandler = useCallback(
    (eventType: string, handler: SSEUpdateHandler) => {
      if (!handlers.current[eventType]) {
        handlers.current[eventType] = [];
      }
      handlers.current[eventType].push(handler);

      /* un‑register fn */
      return () => {
        handlers.current[eventType] = handlers.current[eventType].filter(
          (h) => h !== handler,
        );
      };
    },
    [],
  );

  /* ---------- connect ---------- */
  const connect = useCallback((baseUrl: string) => {
    console.log("[useSSEUpdates] connect called —", { baseUrl });

    if (retryTimeoutRef.current) {
      clearTimeout(retryTimeoutRef.current);
      retryTimeoutRef.current = null;
    }

    if (!baseUrl || typeof baseUrl !== "string") {
      setError("No valid SSE base URL provided (must be a string).");
      return;
    }

    /* close any existing stream first */
    if (eventSource.current && eventSource.current.readyState !== EventSource.CLOSED) {
      console.log("[useSSEUpdates] closing existing EventSource before reconnecting");
      eventSource.current.close();
    }

    /* grab token (optional) */
    const token = localStorage.getItem("access_token");
    const urlWithToken = token ? `${baseUrl}?token=${token}` : baseUrl;

    try {
      const es = new EventSource(urlWithToken, { withCredentials: false });
      eventSource.current = es;

      es.onopen = () => {
        console.log("[useSSEUpdates] stream open");
        setIsConnected(true);
        setError(null);
        retryAttempt.current = 0;
      };

      es.onmessage = (evt) => {
        try {
          const message: MinioObjectCreatedEvent = JSON.parse(evt.data);
          if (message.event_type && handlers.current[message.event_type]) {
            handlers.current[message.event_type].forEach((fn) => {
              try {
                fn(message);
              } catch (e) {
                console.error("[useSSEUpdates] handler error", e);
              }
            });
          }
        } catch (e) {
          console.error("[useSSEUpdates] failed to parse event", e);
        }
      };

      es.onerror = () => {
        console.warn("[useSSEUpdates] stream error/closed, scheduling retry");
        setIsConnected(false);

        if (eventSource.current === es) {
          eventSource.current.close();
          eventSource.current = null;
        }

        retryAttempt.current += 1;
        const delay = Math.min(
          MAX_RETRY_DELAY,
          INITIAL_RETRY_DELAY * 2 ** (retryAttempt.current - 1),
        );
        setError(
          `SSE connection lost. Retrying in ${delay / 1000}s (attempt ${retryAttempt.current})`,
        );

        retryTimeoutRef.current = setTimeout(() => connect(baseUrl), delay);
      };
    } catch (err) {
      console.error("[useSSEUpdates] failed to create EventSource", err);
    }
  }, []);

  /* ---------- disconnect ---------- */
  const disconnect = useCallback(() => {
    if (retryTimeoutRef.current) {
      clearTimeout(retryTimeoutRef.current);
      retryTimeoutRef.current = null;
    }
    if (eventSource.current) {
      console.log("[useSSEUpdates] disconnecting stream");
      eventSource.current.close();
      eventSource.current = null;
      setIsConnected(false);
      retryAttempt.current = 0;
      setError(null);
    }
  }, []);

  /* ---------- cleanup on unmount ---------- */
  useEffect(() => {
    return () => {
      if (eventSource.current) {
        eventSource.current.close();
      }
      if (retryTimeoutRef.current) {
        clearTimeout(retryTimeoutRef.current);
      }
    };
  }, []);

  return { isConnected, error, registerHandler, connect, disconnect };
};
