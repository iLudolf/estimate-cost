import { cancel, isCancel } from '@clack/prompts';
import pc from 'picocolors';
import semver from 'semver';

import { log } from './logger.js';

export function checkNodeVersion(requiredVersion: string) {
    if (!semver.satisfies(process.version, requiredVersion)) {
        log(
            pc.red(
                `You are running Node ${process.version}. ` +
                    `Node ${requiredVersion} or higher is required. ` +
                    'Please update your version of Node.',
            ),
        );
        process.exit(1);
    }
}

/**
 * Checks if the response from a Clack prompt was a cancellation symbol, and if so,
 * ends the interactive process.
 */
export function checkCancel<T>(value: T | symbol): value is T {
    if (isCancel(value)) {
        cancel('Operação cancelada.');
        process.exit(0);
    }
    return true;
}
