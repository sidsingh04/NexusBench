const BASE_URL = '/api/v1';

export class ApiError extends Error {
  status: number;
  code: string;

  constructor(status: number, code: string, message: string) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
    this.code = code;
  }
}

async function handleResponse<T>(res: Response): Promise<T> {
  const data = await res.json().catch(() => null);
  
  if (!res.ok) {
    throw new ApiError(
      res.status,
      data?.code || 'UNKNOWN_ERROR',
      data?.message || 'An unexpected error occurred'
    );
  }
  
  return data as T;
}

export const api = {
  get: async <T>(path: string, adminKey?: string): Promise<T> => {
    const headers: Record<string, string> = {
      'Accept': 'application/json',
    };
    if (adminKey) {
      headers['Authorization'] = `Bearer ${adminKey}`;
    }
    
    const res = await fetch(`${BASE_URL}${path}`, { headers });
    return handleResponse<T>(res);
  },
  
  post: async <T>(path: string, body?: any, adminKey?: string): Promise<T> => {
    const headers: Record<string, string> = {
      'Accept': 'application/json',
    };
    
    if (adminKey) {
      headers['Authorization'] = `Bearer ${adminKey}`;
    }
    
    const isFormData = body instanceof FormData;
    if (!isFormData && body) {
      headers['Content-Type'] = 'application/json';
    }
    
    const res = await fetch(`${BASE_URL}${path}`, {
      method: 'POST',
      headers,
      body: isFormData ? body : JSON.stringify(body),
    });
    
    return handleResponse<T>(res);
  },
  
  getEventSource: (path: string): EventSource => {
    return new EventSource(`${BASE_URL}${path}`);
  }
};
