/*
 * Copyright (c) 2016 MariaDB Corporation Ab
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2024-01-15
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */

/**
 * @file utils.c - General utility functions
 *
 * @verbatim
 * Revision History
 *
 * Date         Who                     Description
 * 10-06-2013   Massimiliano Pinto      Initial implementation
 * 12-06-2013   Massimiliano Pinto      Read function trought
 *                                      the gwbuff strategy
 * 13-06-2013   Massimiliano Pinto      MaxScale local authentication
 *                                      basics
 * 02-09-2014   Martin Brampton         Replaced C++ comments by C comments
 *
 * @endverbatim
 */

#include <maxscale/utils.h>
#include <maxscale/utils.hh>

#include <fcntl.h>
#include <netdb.h>
#include <regex.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/utsname.h>
#include <netinet/tcp.h>
#include <openssl/sha.h>
#include <thread>
#include <curl/curl.h>
#include <crypt.h>

#include <maxbase/alloc.h>
#include <maxscale/config.hh>
#include <maxscale/dcb.hh>
#include <maxscale/limits.h>
#include <maxscale/pcre2.hh>
#include <maxscale/poll.hh>
#include <maxscale/random.h>
#include <maxscale/secrets.hh>
#include <maxscale/session.hh>

#if !defined (PATH_MAX)
# if defined (__USE_POSIX)
#   define PATH_MAX _POSIX_PATH_MAX
# else
#   define PATH_MAX 256
# endif
#endif

#define MAX_ERROR_MSG PATH_MAX

/* used in the hex2bin function */
#define char_val(X) \
    (X >= '0' && X <= '9' ? X - '0'       \
                          : X >= 'A' && X <= 'Z' ? X - 'A' + 10    \
                                                 : X >= 'a' && X <= 'z' ? X - 'a' + 10    \
                                                                        : '\177')

/* used in the bin2hex function */
char hex_upper[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
char hex_lower[] = "0123456789abcdefghijklmnopqrstuvwxyz";

/**
 * Check if the provided pathname is POSIX-compliant. The valid characters
 * are [a-z A-Z 0-9._-].
 * @param path A null-terminated string
 * @return true if it is a POSIX-compliant pathname, otherwise false
 */
bool is_valid_posix_path(char* path)
{
    char* ptr = path;
    while (*ptr != '\0')
    {
        if (isalnum(*ptr) || *ptr == '/' || *ptr == '.' || *ptr == '-' || *ptr == '_')
        {
            ptr++;
        }
        else
        {
            return false;
        }
    }
    return true;
}

/*****************************************
* backend read event triggered by EPOLLIN
*****************************************/

int setnonblocking(int fd)
{
    int fl;

    if ((fl = fcntl(fd, F_GETFL, 0)) == -1)
    {
        MXS_ERROR("Can't GET fcntl for %i, errno = %d, %s.",
                  fd,
                  errno,
                  mxs_strerror(errno));
        return 1;
    }

    if (fcntl(fd, F_SETFL, fl | O_NONBLOCK) == -1)
    {
        MXS_ERROR("Can't SET fcntl for %i, errno = %d, %s",
                  fd,
                  errno,
                  mxs_strerror(errno));
        return 1;
    }
    return 0;
}

int setblocking(int fd)
{
    int fl;

    if ((fl = fcntl(fd, F_GETFL, 0)) == -1)
    {
        MXS_ERROR("Can't GET fcntl for %i, errno = %d, %s.",
                  fd,
                  errno,
                  mxs_strerror(errno));
        return 1;
    }

    if (fcntl(fd, F_SETFL, fl & ~O_NONBLOCK) == -1)
    {
        MXS_ERROR("Can't SET fcntl for %i, errno = %d, %s",
                  fd,
                  errno,
                  mxs_strerror(errno));
        return 1;
    }
    return 0;
}

char* gw_strend(register const char* s)
{
    while (*s++)
    {
    }
    return (char*) (s - 1);
}

/*****************************************
* generate a random char
*****************************************/
static char gw_randomchar()
{
    return (char)((mxs_random() % 78) + 30);
}

/*****************************************
* generate a random string
* output must be pre allocated
*****************************************/
int gw_generate_random_str(char* output, int len)
{
    int i;

    for (i = 0; i < len; ++i)
    {
        output[i] = gw_randomchar();
    }

    output[len] = '\0';

    return 0;
}

/*****************************************
* hex string to binary data
* output must be pre allocated
*****************************************/
int gw_hex2bin(uint8_t* out, const char* in, unsigned int len)
{
    const char* in_end = in + len;

    if (len == 0 || in == NULL)
    {
        return 1;
    }

    while (in < in_end)
    {
        register unsigned char b1 = char_val(*in);
        uint8_t b2 = 0;
        in++;
        b2 = (b1 << 4) | char_val(*in);
        *out++ = b2;

        in++;
    }

    return 0;
}

/*****************************************
* binary data to hex string
* output must be pre allocated
*****************************************/
char* gw_bin2hex(char* out, const uint8_t* in, unsigned int len)
{
    const uint8_t* in_end = in + len;
    if (len == 0 || in == NULL)
    {
        return NULL;
    }

    for (; in != in_end; ++in)
    {
        *out++ = hex_upper[((uint8_t) * in) >> 4];
        *out++ = hex_upper[((uint8_t) * in) & 0x0F];
    }
    *out = '\0';

    return out;
}

/****************************************************
 * fill a preallocated buffer with XOR(str1, str2)
 * XOR between 2 equal len strings
 * note that XOR(str1, XOR(str1 CONCAT str2)) == str2
 * and that  XOR(str1, str2) == XOR(str2, str1)
 *****************************************************/
void gw_str_xor(uint8_t* output, const uint8_t* input1, const uint8_t* input2, unsigned int len)
{
    const uint8_t* input1_end = NULL;
    input1_end = input1 + len;

    while (input1 < input1_end)
    {
        *output++ = *input1++ ^ *input2++;
    }
}

/**********************************************************
* fill a 20 bytes preallocated with SHA1 digest (160 bits)
* for one input on in_len bytes
**********************************************************/
void gw_sha1_str(const uint8_t* in, int in_len, uint8_t* out)
{
    unsigned char hash[SHA_DIGEST_LENGTH];

    SHA1(in, in_len, hash);
    memcpy(out, hash, SHA_DIGEST_LENGTH);
}

/********************************************************
* fill 20 bytes preallocated with SHA1 digest (160 bits)
* for two inputs, in_len and in2_len bytes
********************************************************/
void gw_sha1_2_str(const uint8_t* in, int in_len, const uint8_t* in2, int in2_len, uint8_t* out)
{
    SHA_CTX context;
    unsigned char hash[SHA_DIGEST_LENGTH];

    SHA1_Init(&context);
    SHA1_Update(&context, in, in_len);
    SHA1_Update(&context, in2, in2_len);
    SHA1_Final(hash, &context);

    memcpy(out, hash, SHA_DIGEST_LENGTH);
}


/**
 * node Gets errno corresponding to latest socket error
 *
 * Parameters:
 * @param fd - in, use
 *          socket to examine
 *
 * @return errno
 *
 *
 */
int gw_getsockerrno(int fd)
{
    int eno = 0;
    socklen_t elen = sizeof(eno);

    if (fd <= 0)
    {
        goto return_eno;
    }

    if (getsockopt(fd, SOL_SOCKET, SO_ERROR, (void*)&eno, &elen) != 0)
    {
        eno = 0;
    }

return_eno:
    return eno;
}

namespace maxscale
{
std::string create_hex_sha1_sha1_passwd(const char* passwd)
{
    uint8_t hash1[SHA_DIGEST_LENGTH] = "";
    uint8_t hash2[SHA_DIGEST_LENGTH] = "";

    int hexsize = SHA_DIGEST_LENGTH * 2 + 1;
    char hexpasswd[hexsize];

    /* hash1 is SHA1(real_password) */
    gw_sha1_str((uint8_t*)passwd, strlen(passwd), hash1);
    /* hash2 is the SHA1(input data), where input_data = SHA1(real_password) */
    gw_sha1_str(hash1, SHA_DIGEST_LENGTH, hash2);
    /* dbpass is the HEX form of SHA1(SHA1(real_password)) */
    gw_bin2hex(hexpasswd, hash2, SHA_DIGEST_LENGTH);

    return hexpasswd;
}
}

/**
 * Remove duplicate and trailing forward slashes from a path.
 * @param path Path to clean up
 */
bool clean_up_pathname(char* path)
{
    char* data = path;
    size_t len = strlen(path);

    if (len > PATH_MAX)
    {
        MXS_ERROR("Pathname too long: %s", path);
        return false;
    }

    while (*data != '\0')
    {
        if (*data == '/')
        {
            if (*(data + 1) == '/')
            {
                memmove(data, data + 1, len);
                len--;
            }
            else if (*(data + 1) == '\0' && data != path)
            {
                *data = '\0';
            }
            else
            {
                data++;
                len--;
            }
        }
        else
        {
            data++;
            len--;
        }
    }

    return true;
}

/**
 * @brief Internal helper function for mkdir_all()
 *
 * @param path Path to create
 * @param mask Bitmask to use
 * @return True if directory exists or it was successfully created, false on error
 */
static bool mkdir_all_internal(char* path, mode_t mask)
{
    bool rval = false;

    if (mkdir(path, mask) == -1 && errno != EEXIST)
    {
        if (errno == ENOENT)
        {
            /** Try to create the parent directory */
            char* ndir = strrchr(path, '/');
            if (ndir)
            {
                *ndir = '\0';
                if (mkdir_all_internal(path, mask))
                {
                    /** Creation of the parent directory was successful, try to
                     * create the directory again */
                    *ndir = '/';
                    if (mkdir(path, mask) == 0)
                    {
                        rval = true;
                    }
                    else
                    {
                        MXS_ERROR("Failed to create directory '%s': %d, %s",
                                  path,
                                  errno,
                                  mxs_strerror(errno));
                    }
                }
            }
        }
        else
        {
            MXS_ERROR("Failed to create directory '%s': %d, %s",
                      path,
                      errno,
                      mxs_strerror(errno));
        }
    }
    else
    {
        rval = true;
    }

    return rval;
}

/**
 * @brief Create a directory and any parent directories that do not exist
 *
 *
 * @param path Path to create
 * @param mask Bitmask to use
 * @return True if directory exists or it was successfully created, false on error
 */
bool mxs_mkdir_all(const char* path, int mask)
{
    char local_path[strlen(path) + 1];
    strcpy(local_path, path);

    if (local_path[sizeof(local_path) - 2] == '/')
    {
        local_path[sizeof(local_path) - 2] = '\0';
    }

    return mkdir_all_internal(local_path, (mode_t)mask);
}

/**
 * @brief Replace whitespace with hyphens
 *
 * @param str String to replace
 */
void replace_whitespace(char* str)
{
    for (char* s = str; *s; s++)
    {
        if (isspace(*s))
        {
            *s = '-';
        }
    }
}

/**
 * Replace all whitespace with spaces and squeeze repeating whitespace characters
 *
 * @param str String to squeeze
 * @return Squeezed string
 */
char* squeeze_whitespace(char* str)
{
    char* store = str;
    char* ptr = str;

    /** Remove leading whitespace */
    while (isspace(*ptr) && *ptr != '\0')
    {
        ptr++;
    }

    /** Squeeze all repeating whitespace */
    while (*ptr != '\0')
    {
        while (isspace(*ptr) && isspace(*(ptr + 1)))
        {
            ptr++;
        }

        if (isspace(*ptr))
        {
            *store++ = ' ';
            ptr++;
        }
        else
        {
            *store++ = *ptr++;
        }
    }

    *store = '\0';

    /** Remove trailing whitespace */
    while (store > str && isspace(*(store - 1)))
    {
        store--;
        *store = '\0';
    }

    return str;
}

/**
 * Strip escape characters from a character string.
 * @param String to parse.
 * @return True if parsing was successful, false on errors.
 */
bool strip_escape_chars(char* val)
{
    int cur, end;

    if (val == NULL)
    {
        return false;
    }

    end = strlen(val) + 1;
    cur = 0;

    while (cur < end)
    {
        if (val[cur] == '\\')
        {
            memmove(val + cur, val + cur + 1, end - cur - 1);
            end--;
        }
        cur++;
    }
    return true;
}

bool configure_network_socket(int so, int type)
{
    int one = 1;

    if (type != AF_UNIX)
    {
        if (setsockopt(so, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) != 0
            || setsockopt(so, SOL_SOCKET, SO_KEEPALIVE, &one, sizeof(one)) != 0)
        {
            MXS_ERROR("Failed to set socket option: %d, %s.", errno, mxs_strerror(errno));
            mxb_assert(!true);
            return false;
        }
    }

    return setnonblocking(so) == 0;
}

static bool configure_listener_socket(int so)
{
    int one = 1;

    if (setsockopt(so, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) != 0
        || setsockopt(so, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) != 0)
    {
        MXS_ERROR("Failed to set socket option: %d, %s.", errno, mxs_strerror(errno));
        return false;
    }

#ifdef SO_REUSEPORT
    if (mxs::have_so_reuseport())
    {
        if (setsockopt(so, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one)) != 0)
        {
            MXS_ERROR("Failed to set socket option: %d, %s.", errno, mxs_strerror(errno));
            return false;
        }
    }
#endif

    return setnonblocking(so) == 0;
}

static void set_port(struct sockaddr_storage* addr, uint16_t port)
{
    if (addr->ss_family == AF_INET)
    {
        struct sockaddr_in* ip = (struct sockaddr_in*)addr;
        ip->sin_port = htons(port);
    }
    else if (addr->ss_family == AF_INET6)
    {
        struct sockaddr_in6* ip = (struct sockaddr_in6*)addr;
        ip->sin6_port = htons(port);
    }
    else
    {
        MXS_ERROR("Unknown address family: %d", (int)addr->ss_family);
        mxb_assert(false);
    }
}

int open_network_socket(enum mxs_socket_type type,
                        struct sockaddr_storage* addr,
                        const char* host,
                        uint16_t port)
{
    mxb_assert(type == MXS_SOCKET_NETWORK || type == MXS_SOCKET_LISTENER);
    struct addrinfo* ai = NULL, hint = {};
    int so = 0, rc = 0;
    hint.ai_socktype = SOCK_STREAM;
    hint.ai_family = AF_UNSPEC;
    hint.ai_flags = AI_ALL;

    if ((rc = getaddrinfo(host, NULL, &hint, &ai)) != 0)
    {
        MXS_ERROR("Failed to obtain address for host %s: %s", host, gai_strerror(rc));
        return -1;
    }

    /* Take the first one */
    if (ai)
    {
        if ((so = socket(ai->ai_family, SOCK_STREAM, 0)) == -1)
        {
            MXS_ERROR("Socket creation failed: %d, %s.", errno, mxs_strerror(errno));
        }
        else
        {
            memcpy(addr, ai->ai_addr, ai->ai_addrlen);
            set_port(addr, port);

            if ((type == MXS_SOCKET_NETWORK && !configure_network_socket(so, addr->ss_family))
                || (type == MXS_SOCKET_LISTENER && !configure_listener_socket(so)))
            {
                close(so);
                so = -1;
            }
            else if (type == MXS_SOCKET_LISTENER && bind(so, (struct sockaddr*)addr, sizeof(*addr)) < 0)
            {
                MXS_ERROR("Failed to bind on '%s:%u': %d, %s",
                          host,
                          port,
                          errno,
                          mxs_strerror(errno));
                close(so);
                so = -1;
            }
            else if (type == MXS_SOCKET_NETWORK)
            {
                MXS_CONFIG* config = config_get_global_options();

                if (config->local_address)
                {
                    freeaddrinfo(ai);
                    ai = NULL;

                    if ((rc = getaddrinfo(config->local_address, NULL, &hint, &ai)) == 0)
                    {
                        struct sockaddr_storage local_address = {};

                        memcpy(&local_address, ai->ai_addr, ai->ai_addrlen);


                        if (bind(so, (struct sockaddr*)&local_address, sizeof(local_address)) == 0)
                        {
                            MXS_INFO("Bound connecting socket to \"%s\".", config->local_address);
                        }
                        else
                        {
                            MXS_ERROR("Could not bind connecting socket to local address \"%s\", "
                                      "connecting to server using default local address: %s",
                                      config->local_address,
                                      mxs_strerror(errno));
                        }
                    }
                    else
                    {
                        MXS_ERROR("Could not get address information for local address \"%s\", "
                                  "connecting to server using default local address: %s",
                                  config->local_address,
                                  mxs_strerror(errno));
                    }
                }
            }
        }

        freeaddrinfo(ai);
    }

    return so;
}

static bool configure_unix_socket(int so)
{
    int one = 1;

    if (setsockopt(so, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) != 0)
    {
        MXS_ERROR("Failed to set socket option: %d, %s.", errno, mxs_strerror(errno));
        return false;
    }

    return setnonblocking(so) == 0;
}

int open_unix_socket(enum mxs_socket_type type, struct sockaddr_un* addr, const char* path)
{
    int fd = -1;

    if (strlen(path) > sizeof(addr->sun_path) - 1)
    {
        MXS_ERROR("The path %s specified for the UNIX domain socket is too long. "
                  "The maximum length is %lu.",
                  path,
                  sizeof(addr->sun_path) - 1);
    }
    else if ((fd = socket(AF_UNIX, SOCK_STREAM, 0)) < 0)
    {
        MXS_ERROR("Can't create UNIX socket: %d, %s", errno, mxs_strerror(errno));
    }
    else if (configure_unix_socket(fd))
    {
        addr->sun_family = AF_UNIX;
        strcpy(addr->sun_path, path);

        /* Bind the socket to the Unix domain socket */
        if (type == MXS_SOCKET_LISTENER && bind(fd, (struct sockaddr*)addr, sizeof(*addr)) < 0)
        {
            MXS_ERROR("Failed to bind to UNIX Domain socket '%s': %d, %s",
                      path,
                      errno,
                      mxs_strerror(errno));
            close(fd);
            fd = -1;
        }
    }

    return fd;
}

long get_processor_count()
{
    mxb_assert(sysconf(_SC_NPROCESSORS_ONLN) == std::thread::hardware_concurrency());
    return std::max(std::thread::hardware_concurrency(), 1U);
}

int64_t get_total_memory()
{
    int64_t pagesize = 0;
    int64_t num_pages = 0;
#if defined _SC_PAGESIZE && defined _SC_PHYS_PAGES
    if ((pagesize = sysconf(_SC_PAGESIZE)) <= 0 || (num_pages = sysconf(_SC_PHYS_PAGES)) <= 0)
    {
        MXS_WARNING("Unable to establish total system memory");
        pagesize = 0;
        num_pages = 0;
    }
#else
#error _SC_PAGESIZE and _SC_PHYS_PAGES are not defined
#endif
    mxb_assert(pagesize * num_pages > 0);
    return pagesize * num_pages;
}

namespace maxscale
{

std::string crypt(const std::string& password, const std::string& salt)
{
#if HAVE_GLIBC
    struct crypt_data cdata;
    cdata.initialized = 0;
    return crypt_r(password.c_str(), salt.c_str(), &cdata);
#else
    static std::mutex mxs_crypt_lock;
    std::lock_guard<std::mutex> guard(mxs_crypt_lock);
    std::string pw = crypt(password.c_str(), salt.c_str());
    return pw;
#endif
}

std::string to_hex(uint8_t value)
{
    std::string out;
    out += hex_lower[value >> 4];
    out += hex_lower[value & 0x0F];
    return out;
}

uint64_t get_byteN(const uint8_t* ptr, int bytes)
{
    uint64_t rval = 0;
    mxb_assert(bytes >= 0 && bytes <= (int)sizeof(rval));
    for (int i = 0; i < bytes; i++)
    {
        rval += (uint64_t)ptr[i] << (i * 8);
    }
    return rval;
}

uint8_t* set_byteN(uint8_t* ptr, uint64_t value, int bytes)
{
    mxb_assert(bytes >= 0 && bytes <= (int)sizeof(value));
    for (int i = 0; i < bytes; i++)
    {
        ptr[i] = (uint8_t)(value >> (i * 8));
    }
    return ptr + bytes;
}

int get_kernel_version()
{
    int rval = 0;
    utsname name;

    if (uname(&name) == 0)
    {
        std::istringstream rel {name.release};
        int major = 0;
        int minor = 0;
        int patch = 0;
        char dot;
        rel >> major;
        rel >> dot;
        rel >> minor;
        rel >> dot;
        rel >> patch;

        rval = major * 10000 + minor * 100 + patch;
    }

    return rval;
}

namespace
{
// SO_REUSEPORT was added in Linux 3.9. Even if SO_REUSEPORT is defined it doesn't mean the kernel supports it
// which is why we have to check the kernel version.
static const bool kernel_supports_so_reuseport = get_kernel_version() >= 30900;
}

bool have_so_reuseport()
{
    return kernel_supports_so_reuseport;
}
}
